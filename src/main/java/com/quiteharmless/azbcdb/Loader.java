package com.quiteharmless.azbcdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.MonthDay;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;

public class Loader {
	private Map<Integer, MembershipType> membershipTypeMap;

	private static final Long VISITOR_ID = new Long(99999L);

	private static final String VISITOR_LOOKUP_ID = "v";

	private static final String VISITOR_NAME = "Visitor";

	private static final LocalDate VISITOR_START_DATE = LocalDate.of(2012, 1, 1);

	private static final String APPLICATION_NAME = "AZBC Database Loader";

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);

	private static final String CREDENTIALS_FILE_PATH = "/home/francis/Documents/azbc-credential.json";

	//https://drive.google.com/open?id=1l1geg9QssnWMfTDm2KE1wlB-gIwLAMuBMxOY4E9vi8E
	private static final String SPREADSHEET_ID = "1l1geg9QssnWMfTDm2KE1wlB-gIwLAMuBMxOY4E9vi8E";

	private static final Logger log = LoggerFactory.getLogger(Loader.class);

	private Credential getCredentials() throws IOException {
		try (FileInputStream fin = new FileInputStream(new File(CREDENTIALS_FILE_PATH))) {
			GoogleCredential credential = GoogleCredential.fromStream(fin).createScoped(SCOPES);

			return credential;
		} catch (Exception e) {
			log.error("Exception", e);

			throw e;
		}
	}

	private void loadData() {
		try (
				Connection connection = DriverManager.getConnection("jdbc:sqlite:/home/francis/data/azbcMember.db");
				PreparedStatement selectMembershipTypeSql = connection.prepareStatement("select mbrship_type_id, mbrship_type_nm, mbrship_len, mbrship_len_type_cd, mbrship_auto_renew, mbrship_max_visit from mbrship_type");
		) {
			final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials())
					.setApplicationName(APPLICATION_NAME)
					.build();

			membershipTypeMap = new HashMap<Integer, MembershipType>();
			ResultSet rs = selectMembershipTypeSql.executeQuery();
			long loadId = System.currentTimeMillis();

			while(rs.next()) {
				MembershipType membershipType = new MembershipType();

				membershipType.setId(rs.getInt("mbrship_type_id"));
				membershipType.setDescription(rs.getString("mbrship_type_nm"));
				membershipType.setLength(rs.getInt("mbrship_len"));
				membershipType.setLengthUnit(rs.getString("mbrship_len_type_cd"));
				membershipType.setAutoRenew(rs.getBoolean("mbrship_auto_renew"));
				membershipType.setMaxVisits(rs.getInt("mbrship_max_visit"));

				membershipTypeMap.put(membershipType.getId(), membershipType);
			}

			List<List<Object>> values = service.spreadsheets().values().get(SPREADSHEET_ID, "!A2:N").execute().getValues();

			if ((values == null) || values.isEmpty()) {
				log.warn("No data");
			} else {
				boolean loadSuccessful = loadMembersData(connection, values, loadId);

				if (loadSuccessful) {
					boolean recordLoadSuccessful = recordLoad(connection, loadId);

					if (recordLoadSuccessful) {
						boolean purgeOldMembersDataSuccessful = purgeOldMembersData(connection);

						if (purgeOldMembersDataSuccessful) {
							log.info("Data load was successful, recorded, and old data was purged.");
						} else {
							log.warn("Data load was successful and recorded, but old data was not purged.");
						}
					} else {
						log.error("Data load was successful, but not recorded.  Please check the logs for more information.");
					}
				} else {
					log.error("Data load failed.  Please check the logs for more information.");
				}
			}
		} catch (Exception e) {
			log.error("Exception", e);
		}
	}

	private boolean loadMembersData(Connection connection, List<List<Object>> membersData, long loadId) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/dd/yy");
		ZoneId zoneId = ZoneId.systemDefault();
		LocalDate today = LocalDate.now(zoneId);
		LocalDate infiniteDate = LocalDate.of(9999, 12, 31);
		boolean result = true;

		log.debug("membersData.size() = " + membersData.size());

		try (
				PreparedStatement replaceMemberSql = connection.prepareStatement("insert or replace into mbr(mbr_id, first_nm, last_nm, load_id) values(?, ?, ?, ?)");
				PreparedStatement replaceNoteSql = connection.prepareStatement("insert or replace into mbr_note(mbr_id, mbr_note_txt, load_id) values(?, ?, ?)");
				PreparedStatement replaceMembershipSql = connection.prepareStatement("insert or replace into mbr_mbrship(mbr_id, active_ind, mbrship_type_id, mbr_mbrship_start_dt, mbr_mbrship_end_dt, mbr_hof_id, load_id) values(?, ?, ?, date(?, 'unixepoch'), date(?, 'unixepoch'), ?, ?)");
				PreparedStatement deleteMembershipLookupSql = connection.prepareStatement("delete from mbr_lu where mbr_id=?");
				PreparedStatement replaceMembershipLookupSql = connection.prepareStatement("insert or replace into mbr_lu(mbr_id, mbr_lu_id, load_id) values(?, ?, ?)");
		) {
			connection.setAutoCommit(false);

			for (List<Object> row: membersData) {
				int colSize = row.size();
				String idString = StringUtils.trimToEmpty((String) row.get(0));
				Long id = Long.valueOf(idString);
				String startDateString = (String) row.get(1);
				String firstName = StringUtils.trimToEmpty((String) row.get(2));
				String lastName = StringUtils.trimToEmpty((String) row.get(3));
				String type = StringUtils.trimToEmpty(((String) row.get(4)).toUpperCase());
				String typeLength = StringUtils.trimToEmpty(((String) row.get(5)).toUpperCase());
				Integer isCurrent = Integer.valueOf(((String) row.get(6)).toUpperCase().equals("Y") ? 1 : 0);
				Long hofId = Long.valueOf(0);
				String endDateString = null;
				String note = null;
				String lookupIdString = null;
				boolean isFamily = false;

				if (8 < colSize) {
					try {
						hofId = Long.valueOf(StringUtils.trimToNull((String) row.get(8)) != null ? (String) row.get(8) : "0");
					} catch (NumberFormatException e) {
					}
				}

				if (11 < colSize) {
					endDateString = StringUtils.trimToNull((String) row.get(11));
				}

				if (12 < colSize) {
					note = StringUtils.trimToNull((String) row.get(12));
				}

				if (13 < colSize) {
					lookupIdString = StringUtils.trimToNull((String) row.get(13));
				}

				replaceMemberSql.setLong(1, id);
				replaceMemberSql.setString(2, firstName);
				replaceMemberSql.setString(3, lastName);
				replaceMemberSql.setLong(4, loadId);
				replaceMemberSql.executeUpdate();

				if (StringUtils.isNotBlank(lookupIdString)) {
					replaceMembershipLookupSql.setLong(1, id);
					replaceMembershipLookupSql.setString(2, lookupIdString);
					replaceMembershipLookupSql.setLong(3, loadId);
					replaceMembershipLookupSql.executeUpdate();
				}

				Integer membershipTypeId = 0;
				LocalDate startDate = LocalDate.parse(startDateString, formatter);
				LocalDate endDate = MonthDay.from(startDate).atYear(today.getYear());

				if (typeLength.equals("LIFE") || typeLength.equals("HON")) {
					membershipTypeId = 1;
					endDate = infiniteDate;
				} else if (type.equals("FAM")) {
					isFamily = true;

					if (typeLength.equals("ANN")) {
						membershipTypeId = 2;
						if (StringUtils.isNotBlank(endDateString)) {
							endDate = LocalDate.parse(endDateString, formatter);
						} else {
							if (isCurrent.intValue() == 1) {
								log.error("endDateString is blank");

								endDate = null;
							} else {
								endDate = startDate.plusYears(1L);
							}
						}
					} else if (typeLength.equals("MON")) {
						membershipTypeId = 4;
						if (StringUtils.isNotBlank(endDateString)) {
							endDate = LocalDate.parse(endDateString, formatter);
						} else {
							if (isCurrent.intValue() == 1) {
								endDate = infiniteDate;
							} else {
								endDate = startDate.plusMonths(1L);
							}
						}
					} else if (typeLength.equals("SHT")) {
						membershipTypeId = 6;
						if (StringUtils.isNotBlank(endDateString)) {
							endDate = LocalDate.parse(endDateString, formatter);
						} else {
							if (isCurrent.intValue() == 1) {
								log.error("endDateString is blank");

								endDate = null;
							} else {
								endDate = startDate.plusMonths(1L);
							}
						}
					} else {
						log.error("Unrecognised typeLength:  " + typeLength);
					}
				} else if (type.equals("IND")) {
					if (typeLength.equals("ANN")) {
						membershipTypeId = 3;
						if (StringUtils.isNotBlank(endDateString)) {
							endDate = LocalDate.parse(endDateString, formatter);
						} else {
							if (isCurrent.intValue() == 1) {
								log.error("endDateString is blank");

								endDate = null;
							} else {
								endDate = startDate.plusYears(1L);
							}
						}
					} else if (typeLength.equals("MON")) {
						membershipTypeId = 5;
						if (StringUtils.isNotBlank(endDateString)) {
							endDate = LocalDate.parse(endDateString, formatter);
						} else {
							if (isCurrent.intValue() == 1) {
								endDate = infiniteDate;
							} else {
								endDate = startDate.plusMonths(1L);
							}
						}
					} else if (typeLength.equals("SHT")) {
						membershipTypeId = 7;
						if (StringUtils.isNotBlank(endDateString)) {
							endDate = LocalDate.parse(endDateString, formatter);
						} else {
							if (isCurrent.intValue() == 1) {
								log.error("endDateString is blank");

								endDate = null;
							} else {
								endDate = startDate.plusMonths(1L);
							}
						}
					} else {
						log.error("Unrecognised typeLength:  " + typeLength);
					}
				}

				log.debug(id + "," + firstName + "," + lastName + "," + type + "," + typeLength + "," + isCurrent + ","  + endDateString + "," + hofId + "," + isFamily + "," + (id.equals(hofId)));

				replaceMembershipSql.setLong(1, id);
				replaceMembershipSql.setInt(3, membershipTypeId);
				replaceMembershipSql.setLong(4, startDate.atStartOfDay(zoneId).toEpochSecond());
				replaceMembershipSql.setLong(6, hofId);
				replaceMembershipSql.setLong(7, loadId);

				if (!isFamily || (isFamily && (id.equals(hofId)))) {
					replaceMembershipSql.setInt(2, isCurrent);
					replaceMembershipSql.setLong(5, endDate.atStartOfDay(zoneId).toEpochSecond());
				} else {
					replaceMembershipSql.setObject(2, null, Types.INTEGER);
					replaceMembershipSql.setObject(5, null, Types.BIGINT);
				}

				replaceMembershipSql.executeUpdate();

				if (StringUtils.isNotBlank(note)) {
					replaceNoteSql.setLong(1, id);
					replaceNoteSql.setString(2, note);
					replaceNoteSql.setLong(3, loadId);

					replaceNoteSql.executeUpdate();
				}
			}

			replaceMemberSql.setLong(1, VISITOR_ID);
			replaceMemberSql.setString(2, VISITOR_NAME);
			replaceMemberSql.setString(3, VISITOR_NAME);
			replaceMemberSql.setLong(4, loadId);
			replaceMemberSql.executeUpdate();

			replaceMembershipSql.setLong(1, VISITOR_ID);
			replaceMembershipSql.setInt(2, 1);
			replaceMembershipSql.setInt(3, 1);
			replaceMembershipSql.setLong(4, VISITOR_START_DATE.atStartOfDay(zoneId).toEpochSecond());
			replaceMembershipSql.setLong(5, infiniteDate.atStartOfDay(zoneId).toEpochSecond());
			replaceMembershipSql.setLong(6, Long.valueOf(0));
			replaceMembershipSql.setLong(7, loadId);
			replaceMembershipSql.executeUpdate();

			replaceMembershipLookupSql.setLong(1, VISITOR_ID);
			replaceMembershipLookupSql.setString(2, VISITOR_LOOKUP_ID);
			replaceMembershipLookupSql.setLong(3, loadId);
			replaceMembershipLookupSql.executeUpdate();

			connection.commit();
		} catch (Exception e) {
			log.error("Exception", e);

			try {
				if (connection != null) {
					connection.rollback();
				}
			} catch (SQLException e1) {
				log.error("SQLException", e1);
			}

			result = false;
		}

		return result;
	}

	private boolean recordLoad(Connection connection, long loadId) {
		boolean result = true;

		try (
				PreparedStatement invalidatePreviousLoadsSql = connection.prepareStatement("update loader set active_ind=0");
				PreparedStatement recordLoadSql = connection.prepareStatement("insert into loader(load_id, active_ind, load_ts) values (?, 1, datetime('now'))");
		) {
			connection.setAutoCommit(false);

			invalidatePreviousLoadsSql.executeUpdate();

			recordLoadSql.setLong(1, loadId);
			recordLoadSql.executeUpdate();

			connection.commit();
		} catch (Exception e) {
			log.error("Exception", e);

			try {
				if (connection != null) {
					connection.rollback();
				}
			} catch (SQLException e1) {
				log.error("SQLException", e1);
			}

			result = false;
		}

		return result;
	}

	private boolean purgeOldMembersData(Connection connection) {
		boolean result = true;

		try (
				PreparedStatement purgeOldMemberSql = connection.prepareStatement("delete from mbr where load_id not in (select load_id from loader order by load_ts desc limit 2)");
				PreparedStatement purgeOldMembershipSql = connection.prepareStatement("delete from mbr_mbrship where load_id not in (select load_id from loader order by load_ts desc limit 2)");
				PreparedStatement purgeOldMembershipLookupSql = connection.prepareStatement("delete from mbr_lu where load_id not in (select load_id from loader order by load_ts desc limit 2)");
				PreparedStatement purgeOldNoteSql = connection.prepareStatement("delete from mbr_note where load_id not in (select load_id from loader order by load_ts desc limit 2)");
				PreparedStatement purgeOldLoaderSql = connection.prepareStatement("delete from loader where load_id not in (select load_id from loader order by load_ts desc limit 2)");
		) {
			connection.setAutoCommit(false);

			purgeOldMemberSql.executeUpdate();
			purgeOldMembershipSql.executeUpdate();
			purgeOldMembershipLookupSql.executeUpdate();
			purgeOldNoteSql.executeUpdate();
			purgeOldLoaderSql.executeUpdate();

			connection.commit();
		} catch (Exception e) {
			log.error("Exception", e);

			try {
				if (connection != null) {
					connection.rollback();
				}
			} catch (SQLException e1) {
				log.error("SQLException", e1);
			}

			result = false;
		}

		return result;
	}
	
	public static void main(String[] args) throws ClassNotFoundException {
		Class.forName("org.sqlite.JDBC");

		Loader app = new Loader();

		app.loadData();
	}

	private class MembershipType {
		private int id;

		private String description;

		private int length;

		private String lengthUnit;

		private boolean isAutoRenew;

		private int maxVisits;

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public int getLength() {
			return length;
		}

		public void setLength(int length) {
			this.length = length;
		}

		public String getLengthUnit() {
			return lengthUnit;
		}

		public void setLengthUnit(String lengthUnit) {
			this.lengthUnit = lengthUnit;
		}

		public boolean isAutoRenew() {
			return isAutoRenew;
		}

		public void setAutoRenew(boolean isAutoRenew) {
			this.isAutoRenew = isAutoRenew;
		}

		public int getMaxVisits() {
			return maxVisits;
		}

		public void setMaxVisits(int maxVisits) {
			this.maxVisits = maxVisits;
		}
	}
}

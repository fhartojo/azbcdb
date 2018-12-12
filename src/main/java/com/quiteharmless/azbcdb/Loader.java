package com.quiteharmless.azbcdb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

	private static final String CREDENTIALS_FILE_PATH = "/azbc-database-lo-1544572518845-3e7e107b99d2.json";

	//https://drive.google.com/open?id=1JGOWvtIo_3ScsXaMXzVQvVUn7ZABZYYF5Mm2vkVuAlk
	private static final String SPREADSHEET_ID = "1JGOWvtIo_3ScsXaMXzVQvVUn7ZABZYYF5Mm2vkVuAlk";

	private static final Logger log = LoggerFactory.getLogger(Loader.class);

	private Credential getCredentials() throws IOException {
		InputStream in = Loader.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
		GoogleCredential credential = GoogleCredential.fromStream(in).createScoped(SCOPES);

		return credential;
	}

	private void loadData() {
		try (
				Connection connection = DriverManager.getConnection("jdbc:sqlite:/home/francis/data/azbcMember2.db");
				PreparedStatement selectMembershipTypeSql = connection.prepareStatement("select mbrship_type_id, mbrship_type_nm, mbrship_len, mbrship_len_type_cd, mbrship_auto_renew, mbrship_max_visit from mbrship_type");
				PreparedStatement replaceMemberSql = connection.prepareStatement("insert or replace into mbr(mbr_id, first_nm, last_nm) values(?, ?, ?)");
				PreparedStatement replaceNoteSql = connection.prepareStatement("insert or replace into mbr_note(mbr_id, mbr_note_txt) values(?, ?)");
				PreparedStatement replaceMembershipSql = connection.prepareStatement("insert or replace into mbr_mbrship(mbr_id, active_ind, mbrship_type_id, mbr_mbrship_start_dt, mbr_mbrship_end_dt, mbr_hof_id) values(?, ?, ?, date(?, 'unixepoch'), date(?, 'unixepoch'), ?)");
				PreparedStatement deleteMembershipLookupSql = connection.prepareStatement("delete from mbr_lu where mbr_id=?");
				PreparedStatement replaceMembershipLookupSql = connection.prepareStatement("insert or replace into mbr_lu(mbr_id, mbr_lu_id) values(?, ?)");
		) {
			PreparedStatement deleteTableSql = null;
			final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials())
					.setApplicationName(APPLICATION_NAME)
					.build();

			membershipTypeMap = new HashMap<Integer, MembershipType>();
			ResultSet rs = selectMembershipTypeSql.executeQuery();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yy");
			ZoneId zoneId = ZoneId.systemDefault();
			LocalDate today = LocalDate.now(zoneId);
			LocalDate infiniteDate = LocalDate.of(9999, 12, 31);

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
				log.debug("No data");
			} else {
				log.debug("values.size() = " + values.size());

				deleteTableSql = connection.prepareStatement("delete from mbr");
				deleteTableSql.executeUpdate();
				deleteTableSql.close();
				deleteTableSql = connection.prepareStatement("delete from mbr_mbrship");
				deleteTableSql.executeUpdate();
				deleteTableSql.close();
				deleteTableSql = connection.prepareStatement("delete from mbr_note");
				deleteTableSql.executeUpdate();
				deleteTableSql.close();

				for (List<Object> row: values) {
					int colSize = row.size();
					String idString = StringUtils.trimToEmpty((String) row.get(0));

					if (StringUtils.isNotBlank(idString)) {
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
						replaceMemberSql.executeUpdate();

						if (StringUtils.isNotBlank(lookupIdString)) {
							deleteMembershipLookupSql.setLong(1, id);
							deleteMembershipLookupSql.executeUpdate();

							replaceMembershipLookupSql.setLong(1, id);
							replaceMembershipLookupSql.setString(2, lookupIdString);
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

							replaceNoteSql.executeUpdate();
						}
					} else {
						break;
					}
				}

				replaceMemberSql.setLong(1, VISITOR_ID);
				replaceMemberSql.setString(2, VISITOR_NAME);
				replaceMemberSql.setString(3, VISITOR_NAME);
				replaceMemberSql.executeUpdate();

				replaceMembershipSql.setLong(1, VISITOR_ID);
				replaceMembershipSql.setInt(2, 1);
				replaceMembershipSql.setInt(3, 1);
				replaceMembershipSql.setLong(4, VISITOR_START_DATE.atStartOfDay(zoneId).toEpochSecond());
				replaceMembershipSql.setLong(5, infiniteDate.atStartOfDay(zoneId).toEpochSecond());
				replaceMembershipSql.setLong(6, Long.valueOf(0));
				replaceMembershipSql.executeUpdate();

				replaceMembershipLookupSql.setLong(1, VISITOR_ID);
				replaceMembershipLookupSql.setString(2, VISITOR_LOOKUP_ID);
				replaceMembershipLookupSql.executeUpdate();
			}
		} catch (Exception e) {
			log.error("Exception", e);
		}
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

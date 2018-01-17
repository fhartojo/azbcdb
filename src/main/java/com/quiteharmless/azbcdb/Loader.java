package com.quiteharmless.azbcdb;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.time.LocalDate;
import java.time.MonthDay;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.odftoolkit.simple.SpreadsheetDocument;
import org.odftoolkit.simple.table.Row;
import org.odftoolkit.simple.table.Table;

public class Loader {
	private Map<Integer, MembershipType> membershipTypeMap;

	private static final Long VISITOR_ID = new Long(99999L);

	private static final String VISITOR_LOOKUP_ID = "v";

	private static final String VISITOR_NAME = "Visitor";

	private static final LocalDate VISITOR_START_DATE = LocalDate.of(2012, 1, 1);

	private void loadData(String membershipDocFilename) {
		SpreadsheetDocument membershipDoc = null;
		Connection connection = null;
		PreparedStatement selectMembershipTypeSql = null;
		PreparedStatement deleteTableSql = null;
		PreparedStatement replaceMemberSql = null;
		PreparedStatement replaceNoteSql = null;
		PreparedStatement replaceMembershipSql = null;
		PreparedStatement replaceMembershipLookupSql = null;

		try {
			connection = DriverManager.getConnection("jdbc:sqlite:/home/francis/data/azbcMember.db");
			selectMembershipTypeSql = connection.prepareStatement("select mbrship_type_id, mbrship_type_nm, mbrship_len, mbrship_len_type_cd, mbrship_auto_renew, mbrship_max_visit from mbrship_type");
			replaceMemberSql = connection.prepareStatement("insert or replace into mbr(mbr_id, first_nm, last_nm) values(?, ?, ?)");
			replaceNoteSql = connection.prepareStatement("insert or replace into mbr_note(mbr_id, mbr_note_txt) values(?, ?)");
			replaceMembershipSql = connection.prepareStatement("insert or replace into mbr_mbrship(mbr_id, active_ind, mbrship_type_id, mbr_mbrship_start_dt, mbr_mbrship_end_dt, mbr_hof_id) values(?, ?, ?, date(?, 'unixepoch'), date(?, 'unixepoch'), ?)");
			replaceMembershipLookupSql = connection.prepareStatement("insert or replace into mbr_lu(mbr_id, mbr_lu_id) values(?, ?)");
			membershipTypeMap = new HashMap<Integer, MembershipType>();
			ResultSet rs = selectMembershipTypeSql.executeQuery();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yy");
			ZoneId zoneId = ZoneId.systemDefault();
			LocalDate today = LocalDate.now(zoneId);
			LocalDate infiniteDate = today.plusYears(100L);

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

			File membershipFile = new File(membershipDocFilename);

			if (membershipFile.isFile()) {			
				membershipDoc = SpreadsheetDocument.loadDocument(membershipFile);

				System.out.println("sheetCount = " + membershipDoc.getSheetCount());

				Table membershipTable = membershipDoc.getSheetByIndex(0);

				System.out.println("columnCount = " + membershipTable.getColumnCount() + "; rowCount = " + membershipTable.getRowCount());

				deleteTableSql = connection.prepareStatement("delete from mbr");
				deleteTableSql.executeUpdate();
				deleteTableSql.close();
				deleteTableSql = connection.prepareStatement("delete from mbr_mbrship");
				deleteTableSql.executeUpdate();
				deleteTableSql.close();
				deleteTableSql = connection.prepareStatement("delete from mbr_note");
				deleteTableSql.executeUpdate();
				deleteTableSql.close();
//				deleteTableSql = connection.prepareStatement("delete from mbr_lu");
//				deleteTableSql.executeUpdate();
//				deleteTableSql.close();

				for (int i = 1; i < membershipTable.getRowCount(); i++) {
					Row row = membershipTable.getRowByIndex(i);
					if (StringUtils.isNotBlank(StringUtils.trimToEmpty(row.getCellByIndex(0).getStringValue()))) {
						Long id = Long.valueOf(row.getCellByIndex(0).getStringValue());
						String startDateString = row.getCellByIndex(1).getStringValue();
						String firstName = StringUtils.trimToEmpty(row.getCellByIndex(2).getStringValue());
						String lastName = StringUtils.trimToEmpty(row.getCellByIndex(3).getStringValue());
						String type = StringUtils.trimToEmpty(row.getCellByIndex(4).getStringValue().toUpperCase());
						String typeLength = StringUtils.trimToEmpty(row.getCellByIndex(5).getStringValue().toUpperCase());
						Integer isCurrent = Integer.valueOf(row.getCellByIndex(6).getStringValue().toUpperCase().equals("Y") ? 1 : 0);
						String endDateString = StringUtils.trimToNull(row.getCellByIndex(11).getStringValue());
						String note = StringUtils.trimToNull(row.getCellByIndex(12).getStringValue());
						Long hofId = null;
						boolean isFamily = false;

						try {
							hofId = Long.valueOf(StringUtils.trimToNull(row.getCellByIndex(8).getStringValue()) != null ? row.getCellByIndex(8).getStringValue() : "0");
						} catch (NumberFormatException e) {
							hofId = Long.valueOf(0);
						}

						replaceMemberSql.setLong(1, id);
						replaceMemberSql.setString(2, firstName);
						replaceMemberSql.setString(3, lastName);
						replaceMemberSql.executeUpdate();

//						replaceMembershipLookupSql.setInt(1, id);
//						replaceMembershipLookupSql.setString(2, "");
//						replaceMembershipLookupSql.executeUpdate();

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
										System.err.println("endDateString is blank");

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
										System.err.println("endDateString is blank");

										endDate = null;
									} else {
										endDate = startDate.plusMonths(1L);
									}
								}
							} else {
								System.err.println("Unrecognised typeLength:  " + typeLength);
							}
						} else if (type.equals("IND")) {
							if (typeLength.equals("ANN")) {
								membershipTypeId = 3;
								if (StringUtils.isNotBlank(endDateString)) {
									endDate = LocalDate.parse(endDateString, formatter);
								} else {
									if (isCurrent.intValue() == 1) {
										System.err.println("endDateString is blank");

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
										System.err.println("endDateString is blank");

										endDate = null;
									} else {
										endDate = startDate.plusMonths(1L);
									}
								}
							} else {
								System.err.println("Unrecognised typeLength:  " + typeLength);
							}
						}

						System.out.println(id + "," + firstName + "," + lastName + "," + type + "," + typeLength + "," + isCurrent + "," + hofId + "," + isFamily + "," + (id.equals(hofId)));

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
			} else {
				System.err.println(membershipDocFilename + " is not a readable spreadsheet");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (membershipDoc != null) {
					membershipDoc.close();
				}

				if (deleteTableSql != null) {
					deleteTableSql.close();
				}

				if (replaceMemberSql != null) {
					replaceMemberSql.close();
				}

				if (replaceMembershipSql != null) {
					replaceMembershipSql.close();
				}

				if (replaceNoteSql != null) {
					replaceNoteSql.close();
				}

				if (replaceMembershipLookupSql != null) {
					replaceMembershipLookupSql.close();
				}

				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
	}

	public static void main(String[] args) throws ClassNotFoundException {
		Class.forName("org.sqlite.JDBC");

		Loader app = new Loader();

		app.loadData(args[0]);
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

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
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.odftoolkit.simple.SpreadsheetDocument;
import org.odftoolkit.simple.table.Row;
import org.odftoolkit.simple.table.Table;

public class Loader {
	private Map<Integer, MembershipType> membershipTypeMap;

	private void loadData(String membershipDocFilename) {
		SpreadsheetDocument membershipDoc = null;
		Connection connection = null;
		PreparedStatement selectMembershipTypeSql = null;
		PreparedStatement deleteTableSql = null;
		PreparedStatement replaceMemberSql = null;
//		PreparedStatement replaceFamilySql = null;
		PreparedStatement replaceMembershipSql = null;
		PreparedStatement replaceMembershipLookupSql = null;

		try {
			connection = DriverManager.getConnection("jdbc:sqlite:/home/francis/data/azbcMember.db");
			selectMembershipTypeSql = connection.prepareStatement("select mbrship_type_id, mbrship_type_nm, mbrship_len, mbrship_len_type_cd, mbrship_auto_renew, mbrship_max_visit from mbrship_type");
			replaceMemberSql = connection.prepareStatement("insert or replace into mbr(mbr_id, first_nm, last_nm) values(?, ?, ?)");
//			replaceFamilySql = connection.prepareStatement("insert or replace into mbrship_fmly(mbr_id, mbr_hof_id) values(?, ?)");
			replaceMembershipSql = connection.prepareStatement("insert or replace into mbr_mbrship(mbr_id, active_ind, mbrship_type_id, mbr_mbrship_start_dt, mbr_mbrship_end_dt, mbr_hof_id) values(?, ?, ?, date(?, 'unixepoch'), date(?, 'unixepoch'), ?)");
			replaceMembershipLookupSql = connection.prepareStatement("insert or replace into mbr_lu(mbr_id, mbr_lu_id) values(?, ?)");
			membershipTypeMap = new HashMap<Integer, MembershipType>();
			ResultSet rs = selectMembershipTypeSql.executeQuery();

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

				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yy");
				ZoneId zoneId = ZoneId.systemDefault();
				LocalDate today = LocalDate.now(zoneId);

				deleteTableSql = connection.prepareStatement("delete from mbr");
				deleteTableSql.executeUpdate();
				deleteTableSql.close();
				deleteTableSql = connection.prepareStatement("delete from mbr_mbrship");
				deleteTableSql.executeUpdate();
				deleteTableSql.close();
//				deleteTableSql = connection.prepareStatement("delete from mbr_lu");
//				deleteTableSql.executeUpdate();
//				deleteTableSql.close();

				for (int i = 1; i < membershipTable.getRowCount(); i++) {
					Row row = membershipTable.getRowByIndex(i);
					Long id = Long.valueOf(row.getCellByIndex(0).getStringValue());
					String startDateString = row.getCellByIndex(1).getStringValue();
					String firstName = StringUtils.trimToEmpty(row.getCellByIndex(2).getStringValue());
					String lastName = StringUtils.trimToEmpty(row.getCellByIndex(3).getStringValue());
					String type = row.getCellByIndex(4).getStringValue().toUpperCase();
					String typeLength = row.getCellByIndex(5).getStringValue().toUpperCase();
					Integer isCurrent = Integer.valueOf(row.getCellByIndex(6).getStringValue().toUpperCase().equals("Y") ? 1 : 0);
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

//					replaceMembershipLookupSql.setInt(1, id);
//					replaceMembershipLookupSql.setString(2, "");
//					replaceMembershipLookupSql.executeUpdate();

					Integer membershipTypeId = 0;
					LocalDate startDate = LocalDate.parse(startDateString, formatter);
					long elapsedMonths = ChronoUnit.MONTHS.between(startDate, today);
					LocalDate endDate = MonthDay.from(startDate).atYear(today.getYear());

					if (typeLength.equals("LIFE") || typeLength.equals("HON")) {
						membershipTypeId = 1;
						endDate = startDate.plusYears(membershipTypeMap.get(membershipTypeId).getLength());
					} else if (type.equals("FAM")) {
						isFamily = true;

						if (typeLength.equals("ANN")) {
							membershipTypeId = 2;
							if (isCurrent == 1) {
								if (!endDate.isAfter(today)) {
									endDate = endDate.plusYears(1);
								}
							} else {
								if (elapsedMonths > 12) {
									endDate = startDate.plusMonths(12);
								}
							}
						} else if (typeLength.equals("MON")) {
							membershipTypeId = 4;
							if (isCurrent == 1) {
								if (!endDate.isAfter(today)) {
									endDate = MonthDay.from(startDate).with(today.getMonth()).atYear(today.getYear()).plusMonths(1);
								}
							} else {
								if (elapsedMonths > 2) {
									endDate = startDate.plusMonths(2);
								}
							}
						} else if (typeLength.equals("SHT")) {
							membershipTypeId = 6;
							if (isCurrent == 1) {
								if (!endDate.isAfter(today)) {
									endDate = MonthDay.from(startDate).with(today.getMonth()).atYear(today.getYear()).plusMonths(1);
								}
							} else {
								if (elapsedMonths > 1) {
									endDate = startDate.plusMonths(1);
								}
							}
						} else {
							System.err.println("Unrecognised typeLength:  " + typeLength);
						}
					} else if (type.equals("IND")) {
						if (typeLength.equals("ANN")) {
							membershipTypeId = 3;
							if (isCurrent == 1) {
								if (!endDate.isAfter(today)) {
									endDate = endDate.plusYears(1);
								}
							} else {
								if (elapsedMonths > 12) {
									endDate = startDate.plusMonths(12);
								}
							}
						} else if (typeLength.equals("MON")) {
							membershipTypeId = 5;
							if (isCurrent == 1) {
								if (!endDate.isAfter(today)) {
									endDate = MonthDay.from(startDate).with(today.getMonth()).atYear(today.getYear()).plusMonths(1);
								}
							} else {
								if (elapsedMonths > 2) {
									endDate = startDate.plusMonths(2);
								}
							}
						} else if (typeLength.equals("SHT")) {
							membershipTypeId = 7;
							if (isCurrent == 1) {
								if (!endDate.isAfter(today)) {
									endDate = MonthDay.from(startDate).with(today.getMonth()).atYear(today.getYear()).plusMonths(1);
								}
							} else {
								if (elapsedMonths > 1) {
									endDate = startDate.plusMonths(1);
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
				}
			} else {
				System.out.println(membershipDocFilename + " is not a readable spreadsheet");
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

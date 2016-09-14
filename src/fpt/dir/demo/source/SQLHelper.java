package fpt.dir.demo.source;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SQLHelper {

	public static Connection c = null;
	public static Statement stmt = null;

	public void openConnection() {
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://10.133.28.205/test", "postgres", "");
			System.out.println("Opened database successfully");
			stmt = c.createStatement();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err.println(ex.getClass().getName() + ": " + ex.getMessage());
			System.exit(0);
		}
		System.out.println("Opened database successfully");
	}

	public void closeConnection() {
		try {
			stmt.close();
			c.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean isTableExist(String tableName) {
		try {
			openConnection();
			// check table is exist
			ResultSet rsExist = stmt.executeQuery(
					"SELECT EXISTS ( SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '"
							+ tableName + "' );");

			while (rsExist.next()) {
				String str = rsExist.getString(1);
				if (str.equals("f")) {
					// table is not exist
					return false;
				} else {
					// table is exist
					return true;
				}
			}
			rsExist.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeConnection();
		}
		return false;
	}

	public boolean isColumnChange(String tableName, String[] fields) {
		try {
			openConnection();
			// check column table change
			ResultSet rsColumn = stmt.executeQuery(
					"SELECT column_name as listField FROM information_schema.columns WHERE column_name <> 'id' AND table_name = '"
							+ tableName + "';");
			List<String> listField = new ArrayList<String>();
			while (rsColumn.next()) {
				listField.add(rsColumn.getString("listField"));
			}

			rsColumn.close();
			for (int i = 0; i < fields.length; i++) {
				if (!listField.contains(fields[i])) {
					return true;
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			closeConnection();
		}
		return false;
	}

	public void createNewTable(String tableName, String[] fields) {
		try {
			openConnection();
			String sql = "CREATE TABLE " + tableName + " (id SERIAL PRIMARY KEY,";
			for (int i = 0; i < fields.length; i++) {
				if (i != fields.length - 1) {
					sql += fields[i] + " TEXT NOT NULL,";
				} else {
					sql += fields[i] + " TEXT NOT NULL);";
				}
			}

			stmt.executeUpdate(sql);
		} catch (Exception ex) {
			System.err.println(ex.getClass().getName() + ": " + ex.getMessage());
			System.exit(0);
		} finally {
			closeConnection();
		}
		System.out.println("Table created successfully");

	}

	public void addDataToTable(String tableName, String[] fields, String[] data) {
		try {
			openConnection();

			String sql = "INSERT INTO " + tableName + " ( ";

			for (int i = 0; i < fields.length; i++) {
				if (i != fields.length - 1) {
					sql += fields[i] + ", ";
				} else {
					sql += fields[i] + ") VALUES ( ";
				}
			}

			for (int i = 0; i < data.length; i++) {
				if (i != data.length - 1) {
					sql += "'" + data[i] + "'" + ", ";
				} else {
					sql += "'" + data[i] + "'" + " );";
				}
			}

			stmt.executeUpdate(sql);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			closeConnection();
		}

	}

	public void dropTable(String tableName) {
		try {
			openConnection();
			String sql = "DROP TABLE " + tableName + ";";

			stmt.executeUpdate(sql);
		} catch (Exception ex) {
			System.err.println(ex.getClass().getName() + ": " + ex.getMessage());
			System.exit(0);
		} finally {
			closeConnection();
		}

		System.out.println("Table droped successfully");
	}

	public void deleteData(String tableName) {
		try {
			openConnection();
			String sql = "DELETE FROM " + tableName + ";";
			stmt.executeUpdate(sql);

		} catch (Exception ex) {
			System.err.println(ex.getClass().getName() + ": " + ex.getMessage());
			System.exit(0);
		} finally {
			closeConnection();
		}

		System.out.println("Table deleted successfully");
	}
}

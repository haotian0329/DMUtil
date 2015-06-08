import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;


/*  
 * PreparedStatement介绍
 * 包含于 PreparedStatement 对象中的 SQL 语句可具有一个或多个 IN 参数。
 * IN 参数的值在 SQL 语句创建时未被指定。
 * 相反,该语句为每个 IN 参数保留一个问号(“?”)作为占位符。
 * 每个问号所对应的值必须在该语句执行之前,通过适当的setXXX 方法来提供
 * */

/*
 * ResultSet介绍 
 * 提供执行 SQL 语句后从数据库返回结果中获取数据的方法。
 * 执行 SQL 语句后，数据库返回结果被 JDBC 处理成结果集对象,
 * 通过 Statement,PreparedStatement,CallableStatement 三种不同类型的语句进行查询都可以返回 ResultSet 类型的对象。
 * 可以用 ResultSet 对象的 next 方法以行为单位进行浏览,用 getXXX 方法取出当前行的某一列的值。
 * 方法 getXXX 提供了获取当前行中某列值的途径。在每一行内,可按任何次序获取列值，列名或列号可用于标识要从中获取数据的列。
 */

class DMUtil {

	public DMUtil() {
	}

	// 定义 DM JDBC 驱动串
	private String jdbcString = "dm.jdbc.driver.DmDriver";
	// 定义 DM URL 连接串
	private String urlString = "jdbc:dm://localhost:5236";
	// 定义连接用户名
	private String userName = "SYSDBA";
	// 定义连接用户口令
	private String password = "123456";
	// 定义连接对象
	private Connection conn = null;

	/**
	 * 加载 JDBC 驱动程序
	 * 
	 * @throws SQLException
	 */
	public void loadJdbcDriver() throws SQLException {
		try {
			System.out.println("Loading JDBC Driver...");
			// 加载 JDBC 驱动程序
			Class.forName(jdbcString);
		} catch (ClassNotFoundException e) {
			throw new SQLException("Load JDBC Driver Error : " + e.getMessage());
		} catch (Exception ex) {
			throw new SQLException("Load JDBC Driver Error : "
					+ ex.getMessage());
		}
	}

	/**
	 * 连接 DM 数据库
	 * 
	 * @throws SQLException
	 */
	public void connect() throws SQLException {
		try {
			System.out.println("Connecting to DM Server...");
			// 连接 DM 数据库
			conn = DriverManager.getConnection(urlString, userName, password);
		} catch (SQLException e) {
			throw new SQLException("Connect to DM Server Error : "
					+ e.getMessage());
		}
	}

	/**
	 * 关闭连接
	 * 
	 * @throws SQLException
	 */
	public void disConnect() throws SQLException {
		try {
			// 关闭连接
			conn.close();
		} catch (SQLException e) {
			throw new SQLException("close connection error : " + e.getMessage());
		}
	}

	/**
	 * 数据库更新操作（如更新数据库、删除一行、创建一个新表等）
	 * 
	 * @param sql
	 *            :要执行的语句
	 * @throws SQLException
	 */
	public void dmExcSql(String sql) throws SQLException {
		try {
			System.out.println("Sql exec cmd: " + sql);
			// 创建语句对象
			PreparedStatement pstmt = conn.prepareStatement(sql);
			// 执行语句
			pstmt.executeUpdate();
			// 关闭语句
			pstmt.close();
		} catch (Exception ex) {
			throw new SQLException("Load dmExcSql Driver Error :"
					+ ex.getMessage());
		}
	}

	/**
	 * 查询操作：获取查找语句影响的行数
	 * 
	 * @param sql
	 *            :要执行的语句
	 * @return sql语句影响的行数
	 * @throws SQLException
	 */
	public int getRow(String sql) throws SQLException {
		int rowCount = 0;
		try {
			System.out.println("Sql query getRow cmd: " + sql);
			// 创建语句对象
			Statement stmt = conn.createStatement();
			// 执行查询
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				rowCount++;
			}
			System.out.println("影响的行数：row = " + rowCount);
			rs.close();
			stmt.close();
		} catch (Exception ex) {
			throw new SQLException("Load getRow Driver Error : "
					+ ex.getMessage());
		}
		return rowCount;
	}

	/**
	 * 查询操作：获取查找语句影响的列数
	 * 
	 * @param sql
	 *            :要执行的语句
	 * @return sql语句影响的列数
	 * @throws SQLException
	 */
	public int getColumn(String sql) throws SQLException {
		int colCount;
		try {
			System.out.println("Sql query getColumn cmd: " + sql);
			// 创建语句对象
			Statement stmt = conn.createStatement();
			// 执行查询
			ResultSet rs = stmt.executeQuery(sql);
			// 取得结果集元数据
			ResultSetMetaData rsmd = rs.getMetaData();
			// 取得结果集所包含的列数
			colCount = rsmd.getColumnCount();

			System.out.println("影响的列数：column = " + colCount);
			rs.close();
			stmt.close();
		} catch (Exception ex) {
			throw new SQLException("Load getColumn Driver Error : "
					+ ex.getMessage());
		}
		return colCount;
	}

	/**
	 * 获得表格二维数组数据(此处假设全是String类型)
	 * 
	 * @param sqlStr
	 * @return 二维数组String数据
	 */
	public String[][] getStringData(String sql) {
		int rowNum = 0;
		int colNum = 0;
		try {
			// 获取行数和列数
			rowNum = getRow(sql);
			colNum = getColumn(sql);
		} catch (SQLException e1) {
			System.out
					.println("getRow and getColumn Error: " + e1.getMessage());
		}

		String[][] sTable = new String[rowNum][colNum];
		try {
			System.out.println("Sql query getTableData cmd: " + sql);
			// 创建语句对象
			Statement stmt = conn.createStatement();
			// 执行查询
			ResultSet rs = stmt.executeQuery(sql);
			int rowIndex = 0;
			// 获取数据
			while (rs.next()) {
				for (int colIndex = 0; colIndex < colNum; colIndex++) {
					sTable[rowIndex][colIndex] = rs.getString(1 + colIndex);// ResultSet.getString
				}
				rowIndex++;
			}
			rs.close();
			stmt.close();
		} catch (SQLException e1) {
			System.out.println("getStringData Error: " + e1.getMessage());
		}
		return sTable;
	}

	/**
	 * 插入String数据
	 * 
	 * @param sql
	 *            :插入数据语句
	 * @param data
	 *            :存放字符串数据的数组
	 * @throws SQLException
	 */
	public void putString(String sql, String[] data) throws SQLException {
		// 创建语句对象
		PreparedStatement pstmt = conn.prepareStatement(sql);
		// 为参数赋值
		for (int i = 0; i < data.length; i++) {
			pstmt.setString(i + 1, data[i]);
		}
		// 执行语句
		pstmt.executeUpdate();
		// 关闭语句
		pstmt.close();
	}

	/**
	 * 更新String数据
	 * 
	 * @param sql
	 *            ：更新数据语句
	 * @param data
	 *            ：待更新的数据
	 * @throws SQLException
	 */
	public void updateString(String sql, String data) throws SQLException {
		// String sql = "UPDATE production.product SET name = ?"
		// + "WHERE productid = 11;";
		// 创建语句对象
		PreparedStatement pstmt = conn.prepareStatement(sql);
		// 为参数赋值
		pstmt.setString(1, data);
		// 执行语句
		pstmt.executeUpdate();
		// 关闭语句
		pstmt.close();
	}

	/**
	 * 插入int数据
	 * 
	 * @param sql
	 *            :插入数据语句
	 * @param data
	 *            :存放int数据的数组
	 * @throws SQLException
	 */
	public void putInt(String sql, int[] data) throws SQLException {
		// 创建语句对象
		PreparedStatement pstmt = conn.prepareStatement(sql);
		// 为参数赋值
		for (int i = 0; i < data.length; i++) {
			pstmt.setInt(i + 1, data[i]);
		}
		// 执行语句
		pstmt.executeUpdate();
		// 关闭语句
		pstmt.close();
	}

	/**
	 * 更新int数据
	 * 
	 * @param sql
	 *            ：更新数据语句
	 * @param data
	 *            ：待更新的数据
	 * @throws SQLException
	 */
	public void updateInt(String sql, int data) throws SQLException {
		// String sql = "UPDATE production.product SET nameID = ?"
		// + "WHERE productid = 11;";
		// 创建语句对象
		PreparedStatement pstmt = conn.prepareStatement(sql);
		// 为参数赋值
		pstmt.setInt(1, data);
		// 执行语句
		pstmt.executeUpdate();
		// 关闭语句
		pstmt.close();
	}

}

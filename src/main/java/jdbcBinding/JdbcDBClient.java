/**
 * Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Administrative Contact: dnet-project-office@cs.kuleuven.be
 * Technical Contact: arnaud.schoonjans@student.kuleuven.be
 * 
 * Note: This file is a modified version of the one found at: 
 * https://github.com/brianfrankcooper/YCSB/blob/master/jdbc/src/main/java/com/yahoo/ycsb/db/JdbcDBClient.java
 * 
 * Modification: * Functionality for a sharding the database has been removed.
 * 				 * Functionality had been added for load balancing read request to the slaves nodes
 */

package jdbcBinding;

import com.mysql.jdbc.ReplicationDriver;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 * 
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 * 
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type VARCHAR. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 * 
 * <p>
 * The following options must be passed when using this database client.
 * 
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 * 
 * @author sudipto
 * 
 */
public class JdbcDBClient extends DB implements JdbcDBClientConstants {

	private Connection connection;
	private String tableName = "usertable";
	private boolean initialized = false;
	private Properties props;
	private Integer jdbcFetchSize;
	private static final String DEFAULT_PROP = "";
	private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;

	private static class StatementType 	{

		enum OperationType {
			INSERT(1), DELETE(2), READ(3), UPDATE(4), SCAN(5);
			int internalType;

			private OperationType(int type) {
				internalType = type;
			}

			int getHashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result + internalType;
				return result;
			}
		}

		OperationType operationType;
		int numFields;

		StatementType(OperationType type, int numFields) {
			this.operationType = type;
			this.numFields = numFields;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.numFields;
			result = prime * result + ((this.operationType == null) ? 0 : this.operationType.getHashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StatementType other = (StatementType) obj;
			if (this.numFields != other.numFields)
				return false;
			if (this.operationType != other.operationType)
				return false;
			return true;
		}
	}

	/**
	 * Initialize the database connection and set it up for sending requests to
	 * the database. This must be called once per client.
	 * 
	 * @throws
	 */
	@Override
	public void init() throws DBException {
		if (initialized) {
			System.err.println("Client connection already initialized.");
			return;
		}
		/*
		 * Retrieves parameters passed on the command line
		 */
		props = getProperties();
		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
		//String driverName = props.getProperty(DRIVER_CLASS);

		String jdbcFetchSizeStr = props.getProperty(JDBC_FETCH_SIZE);
		if (jdbcFetchSizeStr != null) {
			try {
				this.jdbcFetchSize = Integer.parseInt(jdbcFetchSizeStr);
			} catch (NumberFormatException nfe) {
				System.err.println("Invalid JDBC fetch size specified: "
						+ jdbcFetchSizeStr);
				throw new DBException(nfe);
			}
		}

		try {
			ReplicationDriver driver = new ReplicationDriver();

			Properties propsForConnection = new Properties();
			// Automatic failover for slave nodes
			propsForConnection.put("autoReconnect", "true");
			// Load balance between the slaves
			propsForConnection.put("roundRobinLoadBalance", "true");
			propsForConnection.put("user", user);
			propsForConnection.put("password", passwd);

			this.connection = driver.connect(urls, propsForConnection);

			/*
			 * read-only = true => read operation is going to be executed =>
			 * Load balance to the slaves read-only = false => update operation
			 * is going to be executed => Send to operation to the master node
			 */
			this.connection.setReadOnly(false);
			this.connection.setAutoCommit(true);

			this.cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();
		} catch (SQLException e) {
			System.err.println("Error in database operation: " + e);
			throw new DBException(e);
		} catch (NumberFormatException e) {
			System.err.println("Invalid value for fieldcount property. " + e);
			throw new DBException(e);
		}
		initialized = true;
	}

	@Override
	public void cleanup() throws DBException {
		try {
			this.connection.close();
		} catch (SQLException e) {
			System.err.println("Error in closing the database connection");
			throw new DBException(e);
		}
	}

	private PreparedStatement createAndCacheInsertStatement(
			StatementType insertType, String key) throws SQLException {
		StringBuilder insert = new StringBuilder("INSERT INTO ");
		insert.append(this.tableName);
		insert.append(" VALUES(?");
		for (int i = 0; i < insertType.numFields; i++) {
			insert.append(",?");
		}
		insert.append(");");
		PreparedStatement insertStatement = this.connection
				.prepareStatement(insert.toString());
		PreparedStatement stmt = this.cachedStatements.putIfAbsent(insertType,
				insertStatement);
		if (stmt == null)
			return insertStatement;
		else
			return stmt;
	}

	private PreparedStatement createAndCacheReadStatement(
			StatementType readType, String key) throws SQLException {
		StringBuilder read = new StringBuilder("SELECT * FROM ");
		read.append(this.tableName);
		read.append(" WHERE ");
		read.append(PRIMARY_KEY);
		read.append(" = ");
		read.append("?;");
		PreparedStatement readStatement = this.connection.prepareStatement(read
				.toString());
		PreparedStatement stmt = this.cachedStatements.putIfAbsent(readType,
				readStatement);
		if (stmt == null)
			return readStatement;
		else
			return stmt;
	}

	private PreparedStatement createAndCacheDeleteStatement(
			StatementType deleteType, String key) throws SQLException {
		StringBuilder delete = new StringBuilder("DELETE FROM ");
		delete.append(this.tableName);
		delete.append(" WHERE ");
		delete.append(PRIMARY_KEY);
		delete.append(" = ?;");
		PreparedStatement deleteStatement = this.connection
				.prepareStatement(delete.toString());
		PreparedStatement stmt = this.cachedStatements.putIfAbsent(deleteType,
				deleteStatement);
		if (stmt == null)
			return deleteStatement;
		else
			return stmt;
	}

	private PreparedStatement createAndCacheUpdateStatement(
			StatementType updateType, String key) throws SQLException {
		StringBuilder update = new StringBuilder("UPDATE ");
		update.append(this.tableName);
		update.append(" SET ");
		for (int i = 1; i <= updateType.numFields; i++) {
			update.append(COLUMN_PREFIX);
			update.append(i);
			update.append("=?");
			if (i < updateType.numFields)
				update.append(", ");
		}
		update.append(" WHERE ");
		update.append(PRIMARY_KEY);
		update.append(" = ?;");
		PreparedStatement insertStatement = this.connection
				.prepareStatement(update.toString());
		PreparedStatement stmt = this.cachedStatements.putIfAbsent(updateType,
				insertStatement);
		if (stmt == null)
			return insertStatement;
		else
			return stmt;
	}

	private PreparedStatement createAndCacheScanStatement(
			StatementType scanType, String key) throws SQLException {
		StringBuilder select = new StringBuilder("SELECT * FROM ");
		select.append(this.tableName);
		select.append(" WHERE ");
		select.append(PRIMARY_KEY);
		select.append(" >= ");
		select.append("? ORDER BY " + PRIMARY_KEY + " LIMIT ?;"); 
		PreparedStatement scanStatement = this.connection
				.prepareStatement(select.toString());
		if (this.jdbcFetchSize != null)
			scanStatement.setFetchSize(this.jdbcFetchSize);
		PreparedStatement stmt = this.cachedStatements.putIfAbsent(scanType,
				scanStatement);
		if (stmt == null)
			return scanStatement;
		else
			return stmt;
	}

	@Override
	public int read(String tableName, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		try {
			// Demarcation for read only command to be executed
			this.connection.setReadOnly(true);
			StatementType type = new StatementType(
					StatementType.OperationType.READ, 1);
			PreparedStatement readStatement = this.cachedStatements.get(type);
			if (readStatement == null) {
				readStatement = createAndCacheReadStatement(type, key);
			}
			readStatement.setString(1, key);
			ResultSet resultSet = readStatement.executeQuery();
			if (!resultSet.next()) {
				resultSet.close();
				return 1;
			}
			if (result != null && fields != null) {
				for (String field : fields) {
					String value = resultSet.getString(field);
					result.put(field, new StringByteIterator(value));
				}
			}
			resultSet.close();
			this.connection.setReadOnly(false);
			return SUCCESS;
		} catch (SQLException e) {
			System.err.println("Error in processing read of table " + tableName
					+ ": " + e);
			return -2;
		}
	}

	@Override
	public int scan(String tableName, String startKey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		if (tableName == null) {
			return -1;
		}
		if (startKey == null) {
			return -1;
		}
		try {
			// Demarcation for read only command to be executed
			this.connection.setReadOnly(true);
			StatementType type = new StatementType(
					StatementType.OperationType.SCAN, 1);
			PreparedStatement scanStatement = this.cachedStatements.get(type);
			if (scanStatement == null) {
				scanStatement = createAndCacheScanStatement(type, startKey);
			}
			scanStatement.setString(1, startKey);
			scanStatement.setInt(2, recordcount);
			ResultSet resultSet = scanStatement.executeQuery();
			for (int i = 0; i < recordcount && resultSet.next(); i++) {
				if (result != null && fields != null) {
					HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
					for (String field : fields) {
						String value = resultSet.getString(field);
						values.put(field, new StringByteIterator(value));
					}
					result.add(values);
				}
			}
			resultSet.close();
			this.connection.setReadOnly(false);
			return SUCCESS;
		} catch (SQLException e) {
			System.err.println("Error in processing scan of table: "
					+ tableName + e);
			return -2;
		}
	}

	@Override
	public int update(String tableName, String key,
			HashMap<String, ByteIterator> values) {
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		try {
			this.connection.setReadOnly(false);
			int numFields = values.size();
			StatementType type = new StatementType(
					StatementType.OperationType.UPDATE, numFields);
			PreparedStatement updateStatement = this.cachedStatements.get(type);
			if (updateStatement == null) {
				updateStatement = createAndCacheUpdateStatement(type, key);
			}
			int index = 1;
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				updateStatement.setString(index++, entry.getValue().toString());
			}
			updateStatement.setString(index, key);
			int result = updateStatement.executeUpdate();
			if (result == 1)
				return SUCCESS;
			else
				return 1;
		} catch (SQLException e) {
			System.err.println("Error in processing update to table: "
					+ tableName + e);
			return -1;
		}
	}

	@Override
	public int insert(String tableName, String key,
			HashMap<String, ByteIterator> values) {
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		try {
			this.connection.setReadOnly(false);
			int numFields = values.size();
			StatementType type = new StatementType(
					StatementType.OperationType.INSERT, numFields);
			PreparedStatement insertStatement = this.cachedStatements.get(type);
			if (insertStatement == null) {
				insertStatement = createAndCacheInsertStatement(type, key);
			}
			insertStatement.setString(1, key);
			int index = 2;
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				String field = entry.getValue().toString();
				insertStatement.setString(index++, field);
			}
			int result = insertStatement.executeUpdate();
			if (result == 1)
				return SUCCESS;
			else
				return 1;
		} catch (SQLException e) {
			System.err.println("Error in processing insert to table: "
					+ tableName + e);
			return -1;
		}
	}

	@Override
	public int delete(String tableName, String key) {
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		try {
			this.connection.setReadOnly(false);
			StatementType type = new StatementType(
					StatementType.OperationType.DELETE, 1);
			PreparedStatement deleteStatement = cachedStatements.get(type);
			if (deleteStatement == null) {
				deleteStatement = createAndCacheDeleteStatement(type, key);
			}
			deleteStatement.setString(1, key);
			int result = deleteStatement.executeUpdate();
			if (result == 1)
				return SUCCESS;
			else
				return 1;
		} catch (SQLException e) {
			System.err.println("Error in processing delete to table: "
					+ tableName + e);
			return -1;
		}
	}
}

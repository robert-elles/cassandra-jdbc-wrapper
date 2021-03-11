/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.adejanovski.cassandra.jdbc;

import static com.github.adejanovski.cassandra.jdbc.DataTypeEnum.DECIMAL;
import static com.github.adejanovski.cassandra.jdbc.Utils.BAD_FETCH_DIR;
import static com.github.adejanovski.cassandra.jdbc.Utils.BAD_FETCH_SIZE;
import static com.github.adejanovski.cassandra.jdbc.Utils.FORWARD_ONLY;
import static com.github.adejanovski.cassandra.jdbc.Utils.MUST_BE_POSITIVE;
import static com.github.adejanovski.cassandra.jdbc.Utils.NO_INTERFACE;
import static com.github.adejanovski.cassandra.jdbc.Utils.VALID_LABELS;
import static com.github.adejanovski.cassandra.jdbc.Utils.WAS_CLOSED_RSLT;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * <p>
 * The Supported Data types in CQL are as follows:
 * </p>
 * <table>
 * <tr>
 * <th>type</th>
 * <th>java type</th>
 * <th>description</th>
 * </tr>
 * <tr>
 * <td>ascii</td>
 * <td>String</td>
 * <td>ASCII character string</td>
 * </tr>
 * <tr>
 * <td>bigint</td>
 * <td>Long</td>
 * <td>64-bit signed long</td>
 * </tr>
 * <tr>
 * <td>blob</td>
 * <td>ByteBuffer</td>
 * <td>Arbitrary bytes (no validation)</td>
 * </tr>
 * <tr>
 * <td>boolean</td>
 * <td>Boolean</td>
 * <td>true or false</td>
 * </tr>
 * <tr>
 * <td>counter</td>
 * <td>Long</td>
 * <td>Counter column (64-bit long)</td>
 * </tr>
 * <tr>
 * <td>date</td>
 * <td>java.sql.Date</td>
 * <td>A date</td>
 * </tr>
 * <tr>
 * <td>decimal</td>
 * <td>BigDecimal</td>
 * <td>Variable-precision decimal</td>
 * </tr>
 * <tr>
 * <td>double</td>
 * <td>Double</td>
 * <td>64-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>float</td>
 * <td>Float</td>
 * <td>32-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>int</td>
 * <td>Integer</td>
 * <td>32-bit signed int</td>
 * </tr>
 * <tr>
 * <td>smallint</td>
 * <td>Short</td>
 * <td>16-bit signed int</td>
 * </tr>
 * <tr>
 * <td>text</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>time</td>
 * <td>java.sql.Time</td>
 * <td>A time</td>
 * </tr>
 * <tr>
 * <td>timestamp</td>
 * <td>java.sql.Timestamp</td>
 * <td>A timestamp</td>
 * </tr>
 * <tr>
 * <td>tinyint</td>
 * <td>Byte</td>
 * <td>A byte</td>
 * </tr>
 * <tr>
 * <td>uuid</td>
 * <td>UUID</td>
 * <td>Type 1 or type 4 UUID</td>
 * </tr>
 * <tr>
 * <td>varchar</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>varint</td>
 * <td>BigInteger</td>
 * <td>Arbitrary-precision integer</td>
 * </tr>
 * </table>
 *
 */
class CassandraResultSet extends AbstractResultSet implements CassandraResultSetExtras {
    private static final Logger logger = LoggerFactory.getLogger(CassandraResultSet.class);

    public static final int DEFAULT_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    public static final int DEFAULT_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    public static final int DEFAULT_HOLDABILITY = ResultSet.HOLD_CURSORS_OVER_COMMIT;

    public static final int MAX_COLUMN_WIDTH = 40;

    private Row currentRow;

    /**
     * The rows iterator.
     */
    private int currentIteratorIndex = 0;
    private Iterator<Row> rowsIterator;
    private ArrayList<Iterator<Row>> rowsIterators;

    int rowNumber = 0;
    // the current row key when iterating through results.
    private byte[] curRowKey = null;

    private final CResultSetMetaData meta;

    private final CassandraStatement statement;

    private int resultSetType;

    private int fetchDirection;

    private int fetchSize;

    private boolean wasNull;

    private com.datastax.driver.core.ResultSet driverResultSet;

    /**
     * no argument constructor.
     */
    CassandraResultSet() {
        statement = null;
        meta = new CResultSetMetaData();
    }

    /**
     * Instantiates a new cassandra result set from a com.datastax.driver.core.ResultSet.
     */
    CassandraResultSet(CassandraStatement statement, com.datastax.driver.core.ResultSet resultSet)
            throws SQLException {
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();
        this.driverResultSet = resultSet;
        this.rowsIterators = Lists.newArrayList();
        this.currentIteratorIndex = 0;

        // Initialize meta-data from schema
        populateMetaData();

        rowsIterator = resultSet.iterator();

        if (hasMoreRows()) {
            populateColumns();
        }

        meta = new CResultSetMetaData();
    }

    /**
     * Instantiates a new cassandra result set from a com.datastax.driver.core.ResultSet.
     */
    CassandraResultSet(CassandraStatement statement,
            ArrayList<com.datastax.driver.core.ResultSet> resultSets) throws SQLException {
        this.statement = statement;
        this.resultSetType = statement.getResultSetType();
        this.fetchDirection = statement.getFetchDirection();
        this.fetchSize = statement.getFetchSize();

        // We have several result sets, but we will use only the first one for metadata needs
        this.driverResultSet = resultSets.get(0);

        currentIteratorIndex = 0;

        rowsIterator = driverResultSet.iterator();
        for (int i = 1; i < resultSets.size(); i++) {
            rowsIterator = Iterators.concat(rowsIterator, resultSets.get(i).iterator()); // this
                                                                                         // leads to
                                                                                         // Stack
                                                                                         // Overflow
                                                                                         // Exception
                                                                                         // when
                                                                                         // there
                                                                                         // are too
                                                                                         // many
                                                                                         // resultSets
        }

        // Initialize to column values from the first row
        if (hasMoreRows()) {
            populateColumns();
        }

        meta = new CResultSetMetaData();
    }

    private final boolean hasMoreRows() {
        return (rowsIterator != null
                && (rowsIterator.hasNext() || (rowNumber == 0 && currentRow != null)));
    }

    private final void populateMetaData() {
        // not used anymore

    }

    private final void populateColumns() {
        currentRow = rowsIterator.next();
    }

    public boolean absolute(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void afterLast() throws SQLException {
        if (resultSetType == TYPE_FORWARD_ONLY)
            throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public void beforeFirst() throws SQLException {
        if (resultSetType == TYPE_FORWARD_ONLY)
            throw new SQLNonTransientException(FORWARD_ONLY);
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    private final void checkIndex(int index) throws SQLException {

        if (currentRow != null) {
            wasNull = currentRow.isNull(index - 1);
            if (currentRow.getColumnDefinitions() != null) {
                if (index < 1 || index > currentRow.getColumnDefinitions().asList().size())
                    throw new SQLSyntaxErrorException(
                            String.format(MUST_BE_POSITIVE, String.valueOf(index)) + " "
                                    + currentRow.getColumnDefinitions().asList().size());
            }
        } else if (driverResultSet != null) {
            if (driverResultSet.getColumnDefinitions() != null) {
                if (index < 1 || index > driverResultSet.getColumnDefinitions().asList().size())
                    throw new SQLSyntaxErrorException(
                            String.format(MUST_BE_POSITIVE, String.valueOf(index)) + " "
                                    + driverResultSet.getColumnDefinitions().asList().size());
            }
        }
    }

    private final void checkName(String name) throws SQLException {
        if (currentRow != null) {
            wasNull = currentRow.isNull(name);
            if (!currentRow.getColumnDefinitions().contains(name))
                throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
        } else if (driverResultSet != null) {
            if (driverResultSet.getColumnDefinitions() != null) {
                if (!driverResultSet.getColumnDefinitions().contains(name))
                    throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
            }
        }
    }

    private final void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLRecoverableException(WAS_CLOSED_RSLT);
        }
    }

    public void clearWarnings() throws SQLException {
        // This implementation does not support the collection of warnings so clearing is a no-op
        // but it is still an exception to call this on a closed connection.
        checkNotClosed();
    }

    public void close() throws SQLException {
        if (!isClosed()) {
            this.statement.close();
        }
    }

    public int findColumn(String name) throws SQLException {
        checkNotClosed();
        checkName(name);
        return currentRow.getColumnDefinitions().getIndexOf(name);
    }

    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public BigDecimal getBigDecimal(int index) throws SQLException {
        checkIndex(index);
        return currentRow.getDecimal(index - 1);
    }

    /** @deprecated */
    public BigDecimal getBigDecimal(int index, int scale) throws SQLException {
        checkIndex(index);
        BigDecimal bd = currentRow.getDecimal(index - 1);
        return (bd == null) ? null : bd.setScale(scale);
    }

    public BigDecimal getBigDecimal(String name) throws SQLException {
        checkName(name);
        return currentRow.getDecimal(name);
    }

    /** @deprecated */
    public BigDecimal getBigDecimal(String name, int scale) throws SQLException {
        checkName(name);
        BigDecimal bd = currentRow.getDecimal(name);
        return (bd == null) ? null : bd.setScale(scale);
    }

    public BigInteger getBigInteger(int index) throws SQLException {
        checkIndex(index);
        return currentRow.getVarint(index - 1);
    }

    public BigInteger getBigInteger(String name) throws SQLException {
        checkName(name);
        return currentRow.getVarint(name);
    }

    public boolean getBoolean(int index) throws SQLException {
        checkIndex(index);
        return currentRow.getBool(index - 1);
    }

    public boolean getBoolean(String name) throws SQLException {
        checkName(name);
        return currentRow.getBool(name);
    }

    public byte getByte(int index) throws SQLException {
        checkIndex(index);
        return currentRow.getByte(index - 1);
    }

    public byte getByte(String name) throws SQLException {
        checkName(name);
        return currentRow.getByte(name);
    }

    public byte[] getBytes(int index) throws SQLException {
        ByteBuffer bb = currentRow.getBytes(index - 1);
        return (bb == null) ? null : bb.array();
    }

    public byte[] getBytes(String name) throws SQLException {
        ByteBuffer bb = currentRow.getBytes(name);
        return (bb == null) ? null : bb.array();
    }

    public int getConcurrency() throws SQLException {
        checkNotClosed();
        return statement.getResultSetConcurrency();
    }

    public Date getDate(int index) throws SQLException {
        checkIndex(index);
        LocalDate date = currentRow.getDate(index - 1);
        return (date == null) ? null : new Date(date.getMillisSinceEpoch());
    }

    public Date getDate(int index, Calendar calendar) throws SQLException {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        return getDate(index - 1);
    }

    public Date getDate(String name) throws SQLException {
        checkName(name);
        LocalDate date = currentRow.getDate(name);
        return (date == null) ? null : new Date(date.getMillisSinceEpoch());
    }

    public Date getDate(String name, Calendar calendar) throws SQLException {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getDate(name);
    }

    public double getDouble(int index) throws SQLException {
        checkIndex(index);

        try {
            if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString()
                    .equals("float")) {
                return currentRow.getFloat(index - 1);
            }
            return currentRow.getDouble(index - 1);
        } catch (InvalidTypeException e) {
            throw new SQLNonTransientException(e);
        }
    }

    @SuppressWarnings("cast")
    public double getDouble(String name) throws SQLException {
        checkName(name);
        try {
            if (currentRow.getColumnDefinitions().getType(name).getName().toString()
                    .equals("float")) {
                return (double) currentRow.getFloat(name);
            }
            return currentRow.getDouble(name);
        } catch (InvalidTypeException e) {
            throw new SQLNonTransientException(e);
        }
    }

    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        return fetchDirection;
    }

    public int getFetchSize() throws SQLException {
        checkNotClosed();
        return fetchSize;
    }

    public float getFloat(int index) throws SQLException {
        checkIndex(index);
        return currentRow.getFloat(index - 1);
    }

    public float getFloat(String name) throws SQLException {
        checkName(name);
        return currentRow.getFloat(name);
    }

    public int getHoldability() throws SQLException {
        checkNotClosed();
        return statement.getResultSetHoldability();
    }

    public int getInt(int index) throws SQLException {
        checkIndex(index);
        try {
            return currentRow.getInt(index - 1);
        } catch (NumberFormatException e) {
            return 0;
        }

    }

    public int getInt(String name) throws SQLException {
        checkName(name);
        try {
            return currentRow.getInt(name);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public byte[] getKey() throws SQLException {
        return curRowKey;
    }

    public List<?> getList(int index) throws SQLException {
        checkIndex(index);
        if (currentRow.getColumnDefinitions().getType(index - 1).isCollection()) {
            try {
                return Lists.newArrayList(currentRow.getList(index - 1,
                        Class.forName(DataTypeEnum
                                .fromCqlTypeName(currentRow.getColumnDefinitions()
                                        .getType(index - 1).getTypeArguments().get(0).getName())
                                .asJavaClass().getCanonicalName())));
            } catch (ClassNotFoundException e) {
                logger.warn("Error while executing getList()", e);
            }
        }
        return currentRow.getList(index - 1, String.class);
    }

    public List<?> getList(String name) throws SQLException {
        checkName(name);
        if (currentRow.getColumnDefinitions().getType(name).isCollection()) {
            try {
                return Lists.newArrayList(currentRow.getList(name,
                        Class.forName(DataTypeEnum
                                .fromCqlTypeName(currentRow.getColumnDefinitions().getType(name)
                                        .getTypeArguments().get(0).getName())
                                .asJavaClass().getCanonicalName())));
            } catch (ClassNotFoundException e) {
                logger.warn("Error while executing getList()", e);
            }
        }
        return currentRow.getList(name, String.class);
    }

    @SuppressWarnings("cast")
    public long getLong(int index) throws SQLException {
        checkIndex(index);
        try {
            if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString()
                    .equals("int")) {
                return (long) currentRow.getInt(index - 1);
            } else if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString()
                    .equals("varint")) {
                BigInteger bi = currentRow.getVarint(index - 1);
                return (bi == null) ? null : bi.longValue();
            } else {
                return currentRow.getLong(index - 1);
            }
        } catch (InvalidTypeException e) {
            throw new SQLNonTransientException(e);
        }

    }

    @SuppressWarnings("cast")
    public long getLong(String name) throws SQLException {
        checkName(name);
        try {
            if (currentRow.getColumnDefinitions().getType(name).getName().toString()
                    .equals("int")) {
                return (long) currentRow.getInt(name);
            } else if (currentRow.getColumnDefinitions().getType(name).getName().toString()
                    .equals("varint")) {
                BigInteger bi = currentRow.getVarint(name);
                return (bi == null) ? null : bi.longValue();
            } else {
                return currentRow.getLong(name);
            }
        } catch (InvalidTypeException e) {
            throw new SQLNonTransientException(e);
        }
    }

    public Map<?, ?> getMap(int index) throws SQLException {
        checkIndex(index);
        return currentRow.getMap(index - 1, String.class, String.class); // TODO: replace with a
                                                                         // real type check
    }

    public Map<?, ?> getMap(String name) throws SQLException {
        checkName(name);

        return currentRow.getMap(name, String.class, String.class); // TODO: replace with a real
                                                                    // type check
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return meta;
    }

    @SuppressWarnings({ "cast", "boxing" })
    public Object getObject(int index) throws SQLException {
        checkIndex(index);
        List<DataType> datatypes = null;

        if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString()
                .equals("udt")) {
            return currentRow.getUDTValue(index - 1);
        }

        if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString()
                .equals("tuple")) {
            return currentRow.getTupleValue(index - 1);
        }

        if (currentRow.getColumnDefinitions().getType(index - 1).isCollection()) {
            datatypes = currentRow.getColumnDefinitions().getType(index - 1).getTypeArguments();
            if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString()
                    .equals("set")) {
                if (datatypes.get(0).getName().toString().equals("udt")) {
                    return Sets.newLinkedHashSet(currentRow.getSet(index - 1,
                            TypesMap.getTypeForComparator("udt").getType()));
                } else if (datatypes.get(0).getName().toString().equals("tuple")) {
                    if (datatypes.get(0).getName().toString().equals("udt")) {
                        return Lists.newArrayList(currentRow.getList(index - 1,
                                TypesMap.getTypeForComparator("udt").getType()));
                    } else if (datatypes.get(0).getName().toString().equals("tuple")) {
                        return Lists.newArrayList(currentRow.getList(index - 1,
                                TypesMap.getTypeForComparator("tuple").getType()));
                    } else {
                        return Lists.newArrayList(currentRow.getList(index - 1, TypesMap
                                .getTypeForComparator(datatypes.get(0).toString()).getType()));
                    }

                } else {
                    return Sets.newLinkedHashSet(currentRow.getSet(index - 1,
                            TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
                }

            }
            if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString()
                    .equals("list")) {
                return Lists.newArrayList(currentRow.getList(index - 1,
                        TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
            }
            if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString()
                    .equals("map")) {

                Class<?> keyType = TypesMap.getTypeForComparator(datatypes.get(0).toString())
                        .getType();
                if (datatypes.get(0).getName().toString().equals("udt")) {
                    keyType = TypesMap.getTypeForComparator("udt").getType();
                } else if (datatypes.get(0).getName().toString().equals("tuple")) {
                    keyType = TypesMap.getTypeForComparator("tuple").getType();

                }

                Class<?> valueType = TypesMap.getTypeForComparator(datatypes.get(1).toString())
                        .getType();
                if (datatypes.get(1).getName().toString().equals("udt")) {
                    valueType = TypesMap.getTypeForComparator("udt").getType();
                } else if (datatypes.get(1).getName().toString().equals("tuple")) {
                    valueType = TypesMap.getTypeForComparator("tuple").getType();

                }

                return Maps.newHashMap(currentRow.getMap(index - 1, keyType, valueType));
            }

        } else {
            String typeName = currentRow.getColumnDefinitions().getType(index - 1).getName()
                    .toString();
            if (typeName.equals("varchar")) {
                return currentRow.getString(index - 1);
            } else if (typeName.equals("ascii")) {
                return currentRow.getString(index - 1);
            } else if (typeName.equals("integer")) {
                return currentRow.getInt(index - 1);
            } else if (typeName.equals("bigint")) {
                return currentRow.getLong(index - 1);
            } else if (typeName.equals("blob")) {
                return currentRow.getBytes(index - 1);
            } else if (typeName.equals("boolean")) {
                return currentRow.getBool(index - 1);
            } else if (typeName.equals("counter")) {
                return currentRow.getLong(index - 1);
            } else if (typeName.equals("date")) {
                return currentRow.getDate(index - 1);
            } else if (typeName.equals("decimal")) {
                return currentRow.getDecimal(index - 1);
            } else if (typeName.equals("double")) {
                return currentRow.getDouble(index - 1);
            } else if (typeName.equals("float")) {
                return currentRow.getFloat(index - 1);
            } else if (typeName.equals("inet")) {
                return currentRow.getInet(index - 1);
            } else if (typeName.equals("int")) {
                return currentRow.getInt(index - 1);
            } else if (typeName.equals("smallint")) {
                return currentRow.getShort(index - 1);
            } else if (typeName.equals("text")) {
                return currentRow.getString(index - 1);
            } else if (typeName.equals("time")) {
                if (currentRow.isNull(index - 1)) {
                    return null;
                }
                return new Time(currentRow.getTime(index - 1) / 1_000_000L);
            } else if (typeName.equals("timestamp")) {
                if (currentRow.isNull(index - 1)) {
                    return null;
                }
                return new Timestamp((currentRow.getTimestamp(index - 1)).getTime());
            } else if (typeName.equals("timeuuid")) {
                return currentRow.getUUID(index - 1);
            } else if (typeName.equals("tinyint")) {
                return currentRow.getByte(index - 1);
            } else if (typeName.equals("uuid")) {
                return currentRow.getUUID(index - 1);
            } else if (typeName.equals("varint")) {
                return currentRow.getVarint(index - 1);
            }
        }

        return null;
    }

    @SuppressWarnings({ "cast", "boxing" })
    public Object getObject(String name) throws SQLException {
        checkName(name);
        List<DataType> datatypes = null;

        if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("udt")) {
            return currentRow.getUDTValue(name);
        }

        if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("tuple")) {
            return currentRow.getTupleValue(name);
        }

        if (currentRow.getColumnDefinitions().getType(name).isCollection()) {
            datatypes = currentRow.getColumnDefinitions().getType(name).getTypeArguments();
            if (currentRow.getColumnDefinitions().getType(name).getName().toString()
                    .equals("set")) {
                if (datatypes.get(0).getName().toString().equals("udt")) {
                    return Sets.newLinkedHashSet(currentRow.getSet(name,
                            TypesMap.getTypeForComparator("udt").getType()));
                } else if (datatypes.get(0).getName().toString().equals("tuple")) {
                    return Sets.newLinkedHashSet(currentRow.getSet(name,
                            TypesMap.getTypeForComparator("tuple").getType()));
                } else {
                    return Sets.newLinkedHashSet(currentRow.getSet(name,
                            TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
                }
            }
            if (currentRow.getColumnDefinitions().getType(name).getName().toString()
                    .equals("list")) {
                if (datatypes.get(0).getName().toString().equals("udt")) {
                    return Lists.newArrayList(currentRow.getList(name,
                            TypesMap.getTypeForComparator("udt").getType()));
                } else if (datatypes.get(0).getName().toString().equals("tuple")) {
                    return Lists.newArrayList(currentRow.getList(name,
                            TypesMap.getTypeForComparator("tuple").getType()));
                } else {
                    return Lists.newArrayList(currentRow.getList(name,
                            TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
                }
            }
            if (currentRow.getColumnDefinitions().getType(name).getName().toString()
                    .equals("map")) {
                Class<?> keyType = TypesMap.getTypeForComparator(datatypes.get(0).toString())
                        .getType();
                if (datatypes.get(0).getName().toString().equals("udt")) {
                    keyType = TypesMap.getTypeForComparator("udt").getType();
                } else if (datatypes.get(0).getName().toString().equals("tuple")) {
                    keyType = TypesMap.getTypeForComparator("tuple").getType();

                }

                Class<?> valueType = TypesMap.getTypeForComparator(datatypes.get(1).toString())
                        .getType();
                if (datatypes.get(1).getName().toString().equals("udt")) {
                    valueType = TypesMap.getTypeForComparator("udt").getType();
                } else if (datatypes.get(1).getName().toString().equals("tuple")) {
                    valueType = TypesMap.getTypeForComparator("tuple").getType();
                }

                return Maps.newHashMap(currentRow.getMap(name, keyType, valueType));
            }
        } else {
            String typeName = currentRow.getColumnDefinitions().getType(name).getName().toString();

            if (typeName.equals("varchar")) {
                return currentRow.getString(name);
            } else if (typeName.equals("ascii")) {
                return currentRow.getString(name);
            } else if (typeName.equals("integer")) {
                return currentRow.getInt(name);
            } else if (typeName.equals("bigint")) {
                return currentRow.getLong(name);
            } else if (typeName.equals("blob")) {
                return currentRow.getBytes(name);
            } else if (typeName.equals("boolean")) {
                return currentRow.getBool(name);
            } else if (typeName.equals("counter")) {
                return currentRow.getLong(name);
            } else if (typeName.equals("date")) {
                return currentRow.getDate(name);
            } else if (typeName.equals("decimal")) {
                return currentRow.getDecimal(name);
            } else if (typeName.equals("double")) {
                return currentRow.getDouble(name);
            } else if (typeName.equals("duration")) {
                // FIXME
                return null;
            } else if (typeName.equals("float")) {
                return currentRow.getFloat(name);
            } else if (typeName.equals("inet")) {
                return currentRow.getInet(name);
            } else if (typeName.equals("int")) {
                return currentRow.getInt(name);
            } else if (typeName.equals("smallint")) {
                return currentRow.getShort(name);
            } else if (typeName.equals("text")) {
                return currentRow.getString(name);
            } else if (typeName.equals("time")) {
                if (currentRow.isNull(name)) {
                    return null;
                }
                return new Time(currentRow.getTime(name) / 1_000_000L);
            } else if (typeName.equals("timestamp")) {
                if (currentRow.isNull(name)) {
                    return null;
                }
                return new Timestamp((currentRow.getTimestamp(name)).getTime());
            } else if (typeName.equals("timeuuid")) {
                return currentRow.getUUID(name);
            } else if (typeName.equals("tinyint")) {
                return currentRow.getByte(name);
            } else if (typeName.equals("uuid")) {
                return currentRow.getUUID(name);
            } else if (typeName.equals("varint")) {
                return currentRow.getVarint(name);
            }

        }

        return null;
    }

    public int getRow() throws SQLException {
        checkNotClosed();
        return rowNumber;
    }

    public Set<?> getSet(int index) throws SQLException {
        checkIndex(index);
        try {
            return currentRow.getSet(index - 1,
                    Class.forName(DataTypeEnum
                            .fromCqlTypeName(currentRow.getColumnDefinitions().getType(index - 1)
                                    .getTypeArguments().get(0).getName())
                            .asJavaClass().getCanonicalName()));
        } catch (ClassNotFoundException e) {
            logger.warn("Error while performing getSet()", e);
        }
        return null;
    }

    public Set<?> getSet(String name) throws SQLException {
        checkName(name);
        try {
            return currentRow.getSet(name,
                    Class.forName(DataTypeEnum
                            .fromCqlTypeName(currentRow.getColumnDefinitions().getType(name)
                                    .getTypeArguments().get(0).getName())
                            .asJavaClass().getCanonicalName()));
        } catch (ClassNotFoundException e) {
            logger.warn("Error while performing getSet()", e);
        }
        return null;
    }

    public short getShort(int index) throws SQLException {
        checkIndex(index);
        return currentRow.getShort(index - 1);
    }

    public short getShort(String name) throws SQLException {
        checkName(name);
        return currentRow.getShort(name);
    }

    public Statement getStatement() throws SQLException {
        checkNotClosed();
        return statement;
    }

    public String getString(int index) throws SQLException {
        checkIndex(index);
        try {
            if (currentRow.getColumnDefinitions().getType(index - 1).isCollection()) {
                Object object = getObject(index);
                return (object == null) ? null : String.valueOf(object);
            }
            return currentRow.getString(index - 1);
        } catch (Exception e) {
            Object object = getObject(index);
            return (object == null) ? null : String.valueOf(object);
        }
    }

    public String getString(String name) throws SQLException {
        checkName(name);
        try {
            if (currentRow.getColumnDefinitions().getType(name).isCollection()) {
                Object object = getObject(name);
                return (object == null) ? null : String.valueOf(object);
            }
            return currentRow.getString(name);
        } catch (Exception e) {
            Object object = getObject(name);
            return (object == null) ? null : String.valueOf(object);
        }
    }

    public Time getTime(int index) throws SQLException {
        checkIndex(index);
        // we have to check this since getTime returns a long
        if (currentRow.isNull(index - 1)) {
            return null;
        }
        return new Time(currentRow.getTime(index - 1) / 1_000_000L);
    }

    public Time getTime(int index, Calendar calendar) throws SQLException {
        checkIndex(index);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(index);
    }

    public Time getTime(String name) throws SQLException {
        checkName(name);
        // we have to check this since getTime returns a long
        if (currentRow.isNull(name)) {
            return null;
        }
        return new Time(currentRow.getTime(name) / 1_000_000L);
    }

    public Time getTime(String name, Calendar calendar) throws SQLException {
        checkName(name);
        // silently ignore the Calendar argument; its a hint we do not need
        return getTime(name);
    }

    public Timestamp getTimestamp(int index) throws SQLException {
        checkIndex(index);
        java.util.Date date = currentRow.getTimestamp(index - 1);
        return (date == null) ? null : new Timestamp(date.getTime());
    }

    public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException {
        checkIndex(index);
        calendar.setTime(currentRow.getTimestamp(index - 1));
        return new Timestamp(calendar.toInstant().toEpochMilli());
    }

    public Timestamp getTimestamp(String name) throws SQLException {
        checkName(name);
        java.util.Date date = currentRow.getTimestamp(name);
        return (date == null) ? null : new Timestamp(date.getTime());
    }

    public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException {
        checkName(name);
        calendar.setTime(currentRow.getTimestamp(name));
        return new Timestamp(calendar.toInstant().toEpochMilli());
    }

    public int getType() throws SQLException {
        checkNotClosed();
        return resultSetType;
    }

    // URL (awaiting some clarifications as to how it is stored in C* ... just a validated Sting in
    // URL format?
    public URL getURL(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public URL getURL(String arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    // These Methods are planned to be implemented soon; but not right now...
    // Each set of methods has a more detailed set of issues that should be considered fully...

    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        // the rationale is there are no warnings to return in this implementation...
        return null;
    }

    public boolean isAfterLast() throws SQLException {
        checkNotClosed();
        return rowNumber == Integer.MAX_VALUE;
    }

    public boolean isBeforeFirst() throws SQLException {
        checkNotClosed();
        return rowNumber == 0;
    }

    public boolean isClosed() throws SQLException {
        if (this.statement == null) {
            return true;
        }
        return this.statement.isClosed();
    }

    public boolean isFirst() throws SQLException {
        checkNotClosed();
        return rowNumber == 1;
    }

    public boolean isLast() throws SQLException {
        checkNotClosed();
        return !rowsIterator.hasNext();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return CassandraResultSetExtras.class.isAssignableFrom(iface);
    }

    // Navigation between rows within the returned set of rows
    // Need to use a list iterator so next() needs completely re-thought

    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public synchronized boolean next() throws SQLException {
        if (hasMoreRows()) {
            // populateColumns is called upon init to set up the metadata fields; so skip first call
            if (rowNumber != 0)
                populateColumns();
            rowNumber++;
            return true;
        }
        rowNumber = Integer.MAX_VALUE;
        return false;
    }

    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    public boolean relative(int arg0) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @SuppressWarnings("boxing")
    public void setFetchDirection(int direction) throws SQLException {
        checkNotClosed();

        if (direction == FETCH_FORWARD || direction == FETCH_REVERSE
                || direction == FETCH_UNKNOWN) {
            if ((getType() == TYPE_FORWARD_ONLY) && (direction != FETCH_FORWARD))
                throw new SQLSyntaxErrorException(
                        "attempt to set an illegal direction : " + direction);
            fetchDirection = direction;
        }
        throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
    }

    @SuppressWarnings("boxing")
    public void setFetchSize(int size) throws SQLException {
        checkNotClosed();
        if (size < 0)
            throw new SQLException(String.format(BAD_FETCH_SIZE, size));
        fetchSize = size;
    }

    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.equals(CassandraResultSetExtras.class))
            return (T) this;

        throw new SQLFeatureNotSupportedException(
                String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    /**
     * RSMD implementation. The metadata returned refers to the column values, not the column names.
     */
    class CResultSetMetaData implements ResultSetMetaData {
        public DataType getDataType(int column) throws SQLException {
            checkIndex(column);
            DataType type = null;
            if (currentRow != null) {
                type = currentRow.getColumnDefinitions().getType(column - 1);
            } else {
                type = driverResultSet.getColumnDefinitions().getType(column - 1);
            }

            return type;
        }

        Definition getDefinition(int column) throws SQLException {
            checkIndex(column);
            Definition col = null;
            if (currentRow != null) {
                col = currentRow.getColumnDefinitions().asList().get(column - 1);
            } else {
                col = driverResultSet.getColumnDefinitions().asList().get(column - 1);
            }
            return col;
        }

        /**
         * return the Cassandra Cluster Name as the Catalog
         */
        public String getCatalogName(int column) throws SQLException {
            return statement.connection.getCatalog();
        }

        public String getColumnClassName(int column) throws SQLException {
            if (currentRow != null) {
                return DataTypeEnum
                        .fromCqlTypeName(
                                currentRow.getColumnDefinitions().getType(column - 1).getName())
                        .asJavaClass().getCanonicalName();
            }
            return DataTypeEnum.fromCqlTypeName(driverResultSet.getColumnDefinitions().asList()
                    .get(column - 1).getType().getName()).asJavaClass().getCanonicalName();

        }

        public int getColumnCount() throws SQLException {
            try {
                if (currentRow != null) {
                    return currentRow.getColumnDefinitions().size();
                }
                return driverResultSet.getColumnDefinitions().size();
            } catch (Exception e) {
                return 0;
            }
        }

        @SuppressWarnings("rawtypes")
        public int getColumnDisplaySize(int column) throws SQLException {
            DataType type = getDataType(column);
            DataTypeEnum name = DataTypeEnum.fromCqlTypeName(type.getName());

            // jOOQ has special handling for BigDecimal types and will correctly
            // handle them if both precision and scaling are 0. This method uses
            // precision to determine max column width which we don't want to
            // use for these types ...

            if (name == DECIMAL) {
                return MAX_COLUMN_WIDTH;
            }

            int size = getPrecision(column);
            // ASCII, TEXT, VARCHAR, etc., will have a precision of Integer.MAX_VALUE
            if (size > MAX_COLUMN_WIDTH) {
                size = MAX_COLUMN_WIDTH;
            }
            return size;
        }

        public String getColumnLabel(int column) throws SQLException {
            return getColumnName(column);
        }

        public String getColumnName(int column) throws SQLException {
            if (currentRow != null) {
                return currentRow.getColumnDefinitions().getName(column - 1);
            }
            return driverResultSet.getColumnDefinitions().asList().get(column - 1).getName();
        }

        public int getColumnType(int column) throws SQLException {
            DataType type = getDataType(column);
            return TypesMap.getTypeForComparator(type.toString()).getJdbcType();

        }

        /**
         * Spec says "database specific type name"; for Cassandra this means the AbstractType.
         */
        public String getColumnTypeName(int column) throws SQLException {
            DataType type = getDataType(column);
            return type.toString();
        }

        public int getPrecision(int column) throws SQLException {
            DataType type = getDataType(column);
            DataTypeEnum name = DataTypeEnum.fromCqlTypeName(type.getName());
            if (name != null) {
                return name.getPrecision();
            }

            return 0;
        }

        public int getScale(int column) throws SQLException {
            return 0;
        }

        /**
         * return the DEFAULT current Keyspace as the Schema Name
         */
        public String getSchemaName(int column) throws SQLException {
            return statement.connection.getSchema();
        }

        public boolean isAutoIncrement(int column) throws SQLException {
             return false;
        }

        public boolean isCaseSensitive(int column) throws SQLException {
            DataType type = getDataType(column);
            DataTypeEnum name = DataTypeEnum.fromCqlTypeName(type.getName());
            if (name != null) {
                return name.jdbcType.isCaseSensitive();
            }

            return false;
        }

        public boolean isCurrency(int column) throws SQLException {
            DataType type = getDataType(column);
            DataTypeEnum name = DataTypeEnum.fromCqlTypeName(type.getName());
            if (name != null) {
                return name.jdbcType.isCurrency();
            }

            return false;
        }

        public boolean isDefinitelyWritable(int column) throws SQLException {
            // checkIndex(column);
            return isWritable(column);
        }

        /**
         * absence is the equivalent of null in Cassandra
         */
        public int isNullable(int column) throws SQLException {
            return ResultSetMetaData.columnNullable;
        }

        public boolean isReadOnly(int column) throws SQLException {
            return column == 0;
        }

        public boolean isSearchable(int column) throws SQLException {
            return false;
        }

        public boolean isSigned(int column) throws SQLException {
            DataType type = getDataType(column);
            DataTypeEnum name = DataTypeEnum.fromCqlTypeName(type.getName());
            if (name != null) {
                return name.jdbcType.isSigned();
            }

            return false;
       }

        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }

        public boolean isWritable(int column) throws SQLException {
            return column > 0;
        }

        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLFeatureNotSupportedException(
                    String.format(NO_INTERFACE, iface.getSimpleName()));
        }

        @Override
        public String getTableName(int column) throws SQLException {
            String tableName = "";
            if (currentRow != null) {
                tableName = currentRow.getColumnDefinitions().getTable(column - 1);
            } else {
                tableName = driverResultSet.getColumnDefinitions().getTable(column - 1);
            }
            return tableName;
        }

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkIndex(columnIndex);
        byte[] bytes = new byte[currentRow.getBytes(columnIndex - 1).remaining()];
        currentRow.getBytes(columnIndex - 1).get(bytes, 0, bytes.length);

        return new ByteArrayInputStream(bytes);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        // TODO Auto-generated method stub
        checkName(columnLabel);
        byte[] bytes = new byte[currentRow.getBytes(columnLabel).remaining()];
        currentRow.getBytes(columnLabel).get(bytes, 0, bytes.length);

        return new ByteArrayInputStream(bytes);
    }

    @SuppressWarnings("unused")
    private boolean hasNextIterator() {
        return currentIteratorIndex < rowsIterators.size() - 1;
    }

    @SuppressWarnings("unused")
    private boolean nextIterator() {
        try {
            currentIteratorIndex++;
            rowsIterator = rowsIterators.get(currentIteratorIndex);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Blob getBlob(int index) throws SQLException {
        checkIndex(index);

        return new javax.sql.rowset.serial.SerialBlob(currentRow.getBytes(index - 1).array());
    }

    @Override
    public Blob getBlob(String columnName) throws SQLException {
        checkName(columnName);

        return new javax.sql.rowset.serial.SerialBlob(currentRow.getBytes(columnName).array());
    }

}

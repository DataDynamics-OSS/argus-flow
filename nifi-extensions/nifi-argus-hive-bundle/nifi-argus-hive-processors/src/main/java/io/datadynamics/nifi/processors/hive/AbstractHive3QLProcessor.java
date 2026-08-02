/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Modifications Copyright 2026 Data Dynamics Inc.
 *
 * Ported from Apache NiFi 1.28.0 (rel/nifi-1.28.0) and adapted for NiFi 2.10:
 *   nifi-nar-bundles/nifi-hive-bundle/nifi-hive3-processors/src/main/java/org/apache/nifi/processors/hive/AbstractHive3QLProcessor.java
 */
package io.datadynamics.nifi.processors.hive;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.nifi.components.PropertyDescriptor;
import io.datadynamics.nifi.services.api.hive.VendorHiveDBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * HiveQL을 다루는 프로세서들이 공통으로 사용하는 데이터와 메서드를 제공하는 추상 기반 클래스.
 * PutHive3QL, SelectHive3QL 등 구체 프로세서가 이 클래스를 상속하여 커넥션 획득, 파라미터 바인딩,
 * 쿼리 파싱을 통한 입출력 테이블 이름 추출 등의 공통 기능을 재사용한다.
 */
public abstract class AbstractHive3QLProcessor extends AbstractSessionFactoryProcessor {

    public static final PropertyDescriptor HIVE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("hive3-dbcp-service")
            .displayName("Hive 데이터베이스 연결 풀링 서비스")
            .description("Hive 데이터베이스에 대한 커넥션을 획득하는 데 사용되는 Hive 컨트롤러 서비스")
            .required(true)
            .identifiesControllerService(VendorHiveDBCPService.class)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("hive3-charset")
            .displayName("문자셋")
            .description("레코드 데이터의 문자 집합(캐릭터셋)을 지정한다.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("hive3-query-timeout")
            .displayName("쿼리 타임아웃")
            .description("드라이버가 쿼리 실행을 기다릴 초 단위 시간을 설정한다. "
                    + "0으로 설정하면 타임아웃이 없음을 의미한다. 참고: 드라이버에 따라 0이 아닌 값을 지원하지 않을 수 있다.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    // hiveql.args.N.type 형태의 FlowFile 속성 이름에서 파라미터 인덱스(N)를 추출하기 위한 패턴
    protected static final Pattern HIVEQL_TYPE_ATTRIBUTE_PATTERN = Pattern.compile("hiveql\\.args\\.(\\d+)\\.type");
    // 속성 값이 유효한 JDBC 타입 숫자인지 검증하기 위한 패턴
    protected static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");
    // 처리 결과 FlowFile에 입력/출력 테이블 목록을 기록할 때 사용하는 속성 이름
    static String ATTR_INPUT_TABLES = "query.input.tables";
    static String ATTR_OUTPUT_TABLES = "query.output.tables";

    /**
     * 주어진 FlowFile에 대해 실행해야 할 HiveQL 구문을 결정한다.
     *
     * @param session  주어진 FlowFile에 접근하는 데 사용할 세션
     * @param flowFile HiveQL 구문을 실행할 대상 FlowFile
     * @return 주어진 FlowFile과 연관된 HiveQL 구문
     */
    protected String getHiveQL(final ProcessSession session, final FlowFile flowFile, final Charset charset) {
        // FlowFile의 내용으로부터 HiveQL을 읽어들인다
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

        // 이 FlowFile에 사용할 PreparedStatement를 만들기 위한 문자열을 생성한다.
        return new String(buffer, charset);
    }

    /**
     * 주어진 FlowFile 속성을 기반으로 PreparedStatement에 필요한 모든 파라미터를 설정한다.
     *
     * @param stmt       파라미터를 설정할 대상 statement
     * @param attributes 파라미터의 인덱스, 값, 타입을 도출할 속성 목록
     * @throws SQLException 해당 setter 호출 시 PreparedStatement가 SQLException을 던지는 경우
     */
    protected int setParameters(int base, final PreparedStatement stmt, int paramCount, final Map<String, String> attributes) throws SQLException {

        Map<Integer, ParameterHolder> parmMap = new TreeMap<Integer, ParameterHolder>();

        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Matcher matcher = HIVEQL_TYPE_ATTRIBUTE_PATTERN.matcher(key);
            if (matcher.matches()) {
                final int parameterIndex = Integer.parseInt(matcher.group(1));
                if (parameterIndex >= base && parameterIndex < base + paramCount) {
                    final boolean isNumeric = NUMBER_PATTERN.matcher(entry.getValue()).matches();
                    if (!isNumeric) {
                        throw new SQLDataException("Value of the " + key + " attribute is '" + entry.getValue() + "', which is not a valid JDBC numeral jdbcType");
                    }

                    final String valueAttrName = "hiveql.args." + parameterIndex + ".value";

                    ParameterHolder ph = new ParameterHolder();
                    int realIndexLoc = parameterIndex - base + 1;

                    ph.jdbcType = Integer.parseInt(entry.getValue());
                    ph.value = attributes.get(valueAttrName);
                    ph.attributeName = valueAttrName;

                    parmMap.put(realIndexLoc, ph);

                }
            }
        }


        // 올바른 개수의 파라미터를 모두 수집하고 정렬했으니, 이제 실제로 설정한다.
        for (final Map.Entry<Integer, ParameterHolder> entry : parmMap.entrySet()) {
            final Integer index = entry.getKey();
            final ParameterHolder ph = entry.getValue();

            try {
                setParameter(stmt, ph.attributeName, index, ph.value, ph.jdbcType);
            } catch (final NumberFormatException nfe) {
                throw new SQLDataException("The value of the " + ph.attributeName + " is '" + ph.value + "', which cannot be converted into the necessary data jdbcType", nfe);
            }
        }
        return base + paramCount;
    }

    /**
     * 주어진 값을 적절한 JDBC 데이터 타입으로 매핑하는 방법을 결정하고, 이를 제공된 PreparedStatement에 파라미터로 설정한다.
     *
     * @param stmt           파라미터를 설정할 PreparedStatement
     * @param attrName       파라미터가 유래한 속성의 이름 - 로깅 목적으로 사용
     * @param parameterIndex 설정할 HiveQL 파라미터의 인덱스
     * @param parameterValue 설정할 HiveQL 파라미터의 값
     * @param jdbcType       설정할 HiveQL 파라미터의 JDBC Type
     * @throws SQLException 해당 setter 호출 시 PreparedStatement가 SQLException을 던지는 경우
     */
    protected void setParameter(final PreparedStatement stmt, final String attrName, final int parameterIndex, final String parameterValue, final int jdbcType) throws SQLException {
        if (parameterValue == null) {
            stmt.setNull(parameterIndex, jdbcType);
        } else {
            try {
                switch (jdbcType) {
                    case Types.BIT:
                    case Types.BOOLEAN:
                        stmt.setBoolean(parameterIndex, Boolean.parseBoolean(parameterValue));
                        break;
                    case Types.TINYINT:
                        stmt.setByte(parameterIndex, Byte.parseByte(parameterValue));
                        break;
                    case Types.SMALLINT:
                        stmt.setShort(parameterIndex, Short.parseShort(parameterValue));
                        break;
                    case Types.INTEGER:
                        stmt.setInt(parameterIndex, Integer.parseInt(parameterValue));
                        break;
                    case Types.BIGINT:
                        stmt.setLong(parameterIndex, Long.parseLong(parameterValue));
                        break;
                    case Types.REAL:
                        stmt.setFloat(parameterIndex, Float.parseFloat(parameterValue));
                        break;
                    case Types.FLOAT:
                    case Types.DOUBLE:
                        stmt.setDouble(parameterIndex, Double.parseDouble(parameterValue));
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        stmt.setBigDecimal(parameterIndex, new BigDecimal(parameterValue));
                        break;
                    case Types.DATE:
                        stmt.setDate(parameterIndex, new Date(Long.parseLong(parameterValue)));
                        break;
                    case Types.TIME:
                        stmt.setTime(parameterIndex, new Time(Long.parseLong(parameterValue)));
                        break;
                    case Types.TIMESTAMP:
                        stmt.setTimestamp(parameterIndex, new Timestamp(Long.parseLong(parameterValue)));
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.LONGVARCHAR:
                        stmt.setString(parameterIndex, parameterValue);
                        break;
                    default:
                        stmt.setObject(parameterIndex, parameterValue, jdbcType);
                        break;
                }
            } catch (SQLException e) {
                // 어떤 속성/파라미터에서 에러가 발생했는지 로깅한 뒤, 상위 레벨에서 처리하도록 다시 던진다
                getLogger().error("Error setting parameter {} to value from {} ({})", parameterIndex, attrName, parameterValue, e);
                throw e;
            }
        }
    }

    // 주어진 HiveQL을 ANTLR 기반 ParseDriver로 파싱하여 AST를 얻고, 그 안에서 참조되는 입력/출력 테이블 이름들을 수집한다.
    // 파싱에 실패해도 프로세서 동작 자체를 막지 않고 경고만 남긴 뒤 빈 집합을 반환한다.
    protected Set<TableName> findTableNames(final String query) {
        final ASTNode node;
        try {
            node = new ParseDriver().parse(normalize(query));
        } catch (ParseException e) {
            // 쿼리 파싱에 실패해도 메시지만 로깅하고 계속 진행한다.
            getLogger().debug("Failed to parse query: {} due to {}", query, e, e);
            return Collections.emptySet();
        }
        final HashSet<TableName> tableNames = new HashSet<>();
        findTableNames(node, tableNames);
        return tableNames;
    }

    /**
     * 쿼리를 정규화한다.
     * Hive는 쿼리를 실행하기 전에 PreparedStatement의 파라미터를 미리 치환하는데,
     * 자세한 내용은 {@link org.apache.hive.jdbc.HivePreparedStatement#updateSql(String, HashMap)} 를 참고한다.
     * HiveParser는 쿼리 문자열에 '?'가 있는 것을 예상하지 않으며, 있을 경우 예외를 던진다.
     * 이 normalize 메서드에서는 이를 피하기 위해 '?'를 'x'로 치환한다.
     */
    private String normalize(String query) {
        return query.replace('?', 'x');
    }

    // AST를 재귀적으로 순회하며 TOK_TABNAME 노드를 찾아 테이블 이름(및 DB명, 입력/출력 여부)을 추출한다.
    private void findTableNames(final Object obj, final Set<TableName> tableNames) {
        if (!(obj instanceof CommonTree)) {
            return;
        }
        final CommonTree tree = (CommonTree) obj;
        final int childCount = tree.getChildCount();
        if ("TOK_TABNAME".equals(tree.getText())) {
            final TableName tableName;
            final boolean isInput = "TOK_TABREF".equals(tree.getParent().getText());
            switch (childCount) {
                case 1:
                    tableName = new TableName(null, tree.getChild(0).getText(), isInput);
                    break;
                case 2:
                    tableName = new TableName(tree.getChild(0).getText(), tree.getChild(1).getText(), isInput);
                    break;
                default:
                    throw new IllegalStateException("TOK_TABNAME does not have expected children, childCount=" + childCount);
            }
            // 부모 노드가 TOK_TABREF이면 입력 테이블이다.
            tableNames.add(tableName);
            return;
        }
        for (int i = 0; i < childCount; i++) {
            findTableNames(tree.getChild(i), tableNames);
        }
    }

    // 수집된 테이블 이름 집합을 입력/출력 여부에 따라 query.input.tables / query.output.tables 속성 값(콤마 구분 문자열)으로 변환한다.
    protected Map<String, String> toQueryTableAttributes(Set<TableName> tableNames) {
        final Map<String, String> attributes = new HashMap<>();
        for (TableName tableName : tableNames) {
            final String attributeName = tableName.isInput() ? ATTR_INPUT_TABLES : ATTR_OUTPUT_TABLES;
            if (attributes.containsKey(attributeName)) {
                attributes.put(attributeName, attributes.get(attributeName) + "," + tableName);
            } else {
                attributes.put(attributeName, tableName.toString());
            }
        }
        return attributes;
    }

    // 쿼리 파싱으로 추출한 테이블 정보(데이터베이스명, 테이블명, 입력/출력 여부)를 담는 값 객체
    protected static class TableName {
        private final String database;
        private final String table;
        private final boolean input;

        TableName(String database, String table, boolean input) {
            this.database = database;
            this.table = table;
            this.input = input;
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        public boolean isInput() {
            return input;
        }

        @Override
        public String toString() {
            return database == null || database.isEmpty() ? table : database + '.' + table;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TableName tableName = (TableName) o;

            if (input != tableName.input) return false;
            if (!Objects.equals(database, tableName.database)) return false;
            return table.equals(tableName.table);
        }

        @Override
        public int hashCode() {
            int result = database != null ? database.hashCode() : 0;
            result = 31 * result + table.hashCode();
            result = 31 * result + (input ? 1 : 0);
            return result;
        }
    }

    // setParameters()에서 파라미터를 인덱스 순으로 정렬해 처리하기 위해 속성 이름/JDBC 타입/값을 임시로 묶어두는 홀더
    private class ParameterHolder {
        String attributeName;
        int jdbcType;
        String value;
    }
}

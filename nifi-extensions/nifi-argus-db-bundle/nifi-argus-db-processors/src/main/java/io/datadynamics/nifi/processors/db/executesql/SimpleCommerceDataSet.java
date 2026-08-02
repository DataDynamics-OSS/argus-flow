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
 *   nifi-nar-bundles/nifi-extension-utils/nifi-database-test-utils/src/main/java/org/apache/nifi/util/db/SimpleCommerceDataSet.java
 */
package io.datadynamics.nifi.processors.db.executesql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * 테스트용 샘플 데이터 집합을 생성하는 유틸리티 클래스. 'persons', 'products', 'relationships'
 * 세 개의 테이블로 구성되며, 임의의(random) 이름과 코드 값을 채워 넣은 테스트 데이터를 만든다.
 * 단위/통합 테스트에서 실제 데이터베이스와 유사한 상태를 준비하는 용도로 사용된다.
 */
public class SimpleCommerceDataSet {

    static String dropPersons = "drop table persons";
    static String dropProducts = "drop table products";
    static String dropRelationships = "drop table relationships";
    static String createPersons = "create table persons (id integer, name varchar(100), code integer)";
    static String createProducts = "create table products (id integer, name varchar(100), code integer)";
    static String createRelationships = "create table relationships (id integer,name varchar(100), code integer)";
    static Random rng = new Random(53495);

    /**
     * 지정된 개수만큼 persons, products, relationships 테이블에 임의의(random) 데이터를 채워 넣는다.
     * 기존 테이블이 있으면 먼저 삭제한 뒤 재생성한다.
     */
    public static void loadTestData2Database(Connection con, int nrOfPersons, int nrOfProducts, int nrOfRels) throws SQLException {

        System.out.println(createRandomName());
        System.out.println(createRandomName());
        System.out.println(createRandomName());

        final Statement st = con.createStatement();

        // 테이블이 존재하지 않을 수 있으나, 심각한 문제는 아니므로 무시한다.
        try {
            st.executeUpdate(dropPersons);
        } catch (final Exception ignored) {
        }

        try {
            st.executeUpdate(dropProducts);
        } catch (final Exception ignored) {
        }

        try {
            st.executeUpdate(dropRelationships);
        } catch (final Exception ignored) {
        }

        st.executeUpdate(createPersons);
        st.executeUpdate(createProducts);
        st.executeUpdate(createRelationships);

        for (int i = 0; i < nrOfPersons; i++)
            loadPersons(st, i);

        for (int i = 0; i < nrOfProducts; i++)
            loadProducts(st, i);

        for (int i = 0; i < nrOfRels; i++)
            loadRelationships(st, i);

        st.close();
    }

    static private void loadPersons(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into persons values (" + nr + ", '" + createRandomName() + "', " + rng.nextInt(469946) + ")");
    }

    static private void loadProducts(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into products values (" + nr + ", '" + createRandomName() + "', " + rng.nextInt(469946) + ")");
    }

    static private void loadRelationships(Statement st, int nr) throws SQLException {
        st.executeUpdate("insert into relationships values (" + nr + ", '" + createRandomName() + "', " + rng.nextInt(469946) + ")");
    }

    static private String createRandomName() {
        return createRandomString() + " " + createRandomString();
    }

    static private String createRandomString() {

        final int length = rng.nextInt(10);
        final String characters = "ABCDEFGHIJ";

        final char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }

    private Connection createConnection(String location) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        return DriverManager.getConnection("jdbc:derby:" + location + ";create=true");
    }

}
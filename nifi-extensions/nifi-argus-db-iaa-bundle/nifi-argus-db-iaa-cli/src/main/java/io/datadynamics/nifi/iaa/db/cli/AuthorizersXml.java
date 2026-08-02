/*
 * Copyright 2026 Data Dynamics Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datadynamics.nifi.iaa.db.cli;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * {@code conf/authorizers.xml} 에서 DB 프로바이더의 접속 설정을 읽는다.
 *
 * <p>CLI 가 JDBC URL 을 인자로 또 받으면 설정이 두 곳에 생겨 어긋난다. NiFi 의
 * {@code set-single-user-credentials} 가 {@code nifi.properties} 를 읽는 것과 같은 방식이다.
 */
public final class AuthorizersXml {

    private AuthorizersXml() {
    }

    /**
     * 지정한 {@code <userGroupProvider>} 의 property 들을 읽는다.
     *
     * @param file       authorizers.xml 경로
     * @param identifier 찾을 프로바이더 identifier. {@code null} 이면 DbUserGroupProvider 를
     *                   class 로 하는 첫 번째 프로바이더
     * @throws CliException 파일이 없거나, 해당 프로바이더가 없거나, 파싱에 실패한 경우
     */
    public static Map<String, String> readUserGroupProvider(final File file, final String identifier) {
        if (!file.isFile()) {
            throw new CliException("authorizers.xml 을 찾을 수 없습니다: " + file.getAbsolutePath()
                    + " (--conf 로 경로를 지정할 수 있습니다)");
        }
        final Document document = parse(file);
        final NodeList providers = document.getElementsByTagName("userGroupProvider");

        for (int i = 0; i < providers.getLength(); i++) {
            final Element provider = (Element) providers.item(i);
            final String id = text(provider, "identifier");
            final String clazz = text(provider, "class");
            final boolean matches = identifier != null
                    ? identifier.equals(id)
                    : clazz != null && clazz.endsWith("DbUserGroupProvider");
            if (matches) {
                return properties(provider);
            }
        }
        throw new CliException(identifier != null
                ? "authorizers.xml 에 identifier 가 '" + identifier + "' 인 userGroupProvider 가 없습니다."
                : "authorizers.xml 에서 DbUserGroupProvider 를 찾을 수 없습니다. "
                        + "블록의 주석을 해제했는지 확인하거나 --provider 로 identifier 를 지정하십시오.");
    }

    private static Map<String, String> properties(final Element provider) {
        final Map<String, String> properties = new LinkedHashMap<>();
        final NodeList nodes = provider.getElementsByTagName("property");
        for (int i = 0; i < nodes.getLength(); i++) {
            final Element property = (Element) nodes.item(i);
            final String name = property.getAttribute("name");
            if (!name.isEmpty()) {
                properties.put(name, property.getTextContent() == null
                        ? "" : property.getTextContent().trim());
            }
        }
        return properties;
    }

    private static String text(final Element parent, final String tag) {
        final NodeList nodes = parent.getElementsByTagName(tag);
        for (int i = 0; i < nodes.getLength(); i++) {
            final Node node = nodes.item(i);
            if (node.getParentNode() == parent) {
                return node.getTextContent() == null ? null : node.getTextContent().trim();
            }
        }
        return null;
    }

    private static Document parse(final File file) {
        try {
            final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // 설정 파일을 파싱할 뿐이지만 XXE 를 막아 둔다 — 이 파일은 자격증명을 담는다.
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            final DocumentBuilder builder = factory.newDocumentBuilder();
            return builder.parse(file);
        } catch (final ParserConfigurationException | SAXException | IOException e) {
            throw new CliException("authorizers.xml 을 읽을 수 없습니다: " + e.getMessage(), e);
        }
    }
}

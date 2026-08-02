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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/ExecuteStreamCommand.java
 */
package io.datadynamics.nifi.processors.command;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.*;
import java.lang.ProcessBuilder.Redirect;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * 이 프로세서는 FlowFile의 콘텐츠를 대상으로 외부 명령(command)을 실행하고,
 * 그 명령의 실행 결과를 담은 새로운 FlowFile을 생성한다.
 * 또한 Command Path/Command Arguments 실행 전후에 각각 Pre-Command / Post-Command를
 * 추가로 실행할 수 있도록 확장되어 있다(하나의 셸 스크립트로 묶여 순차 실행됨).
 * </p>
 * <p>
 * <strong>주요 속성(Properties):</strong>
 * </p>
 * <ul>
 * <li><strong>Command Path</strong>
 * <ul>
 * <li>실행할 명령을 지정한다; 실행 파일 이름만 지정하는 경우 사용자 환경의 PATH에 존재해야 한다.</li>
 * <li>기본값: 없음</li>
 * <li>Expression Language 지원: true</li>
 * </ul>
 * </li>
 * <li>Arguments Strategy
 * <ul>
 * <li>실행 파일에 전달할 인자를 구성하는 전략을 선택한다</li>
 * <ul>
 * <li>Command Arguments Property: Command Arguments 속성에서 구분자로 나뉜 인자 목록을 사용한다. 인자 안의 따옴표는 지원하지 않는다.</li>
 * <li>Dynamic Property Arguments: 동적 속성(Dynamic Properties)을 사용하며, 각 속성이 하나의 인자가 된다. 따옴표를 지원한다.</li>
 * </ul>
 * </ul>
 * </li>
 * <li>Command Arguments
 * <ul>
 * <li>실행 파일에 전달할 인자들을 ';' 문자로 구분하여 지정한다. 각 인자는 Expression Language 구문이 될 수 있다.</li>
 * <li>기본값: 없음</li>
 * <li>Expression Language 지원: true</li>
 * </ul>
 * </li>
 * <li>Working Directory
 * <ul>
 * <li>명령 실행 시 현재 작업 디렉토리로 사용할 디렉토리</li>
 * <li>기본값: 없음(지정하지 않으면 NiFi의 현재 작업 디렉토리, 대체로 NiFi 설치 루트 디렉토리가 사용됨)</li>
 * <li>Expression Language 지원: true</li>
 * </ul>
 * </li>
 * <li>Ignore STDIN
 * <ul>
 * <li>FlowFile의 콘텐츠를 STDIN으로 스트리밍할지 여부를 나타낸다</li>
 * <li>기본값: false(즉, 기본적으로 FlowFile의 콘텐츠가 명령의 STDIN으로 전달됨)</li>
 * <li>Expression Language 지원: false</li>
 * </ul>
 * </li>
 * </ul>
 *
 * <p>
 * <strong>관계(Relationships):</strong>
 * </p>
 * <ul>
 * <li>original
 * <ul>
 * <li>원본으로 유입된 FlowFile이 전달되는 목적지</li>
 * </ul>
 * </li>
 * <li>output-stream
 * <ul>
 * <li>종료 코드(exit code)가 0인 경우, 명령의 출력으로 생성된 FlowFile이 전달되는 목적지</li>
 * </ul>
 * </li>
 * <li>nonzero-status
 * <ul>
 * <li>종료 코드(exit code)가 0이 아닌 경우, 명령의 출력으로 생성된 FlowFile이 전달되는 목적지</li>
 * </ul>
 * </li>
 * </ul>
 * <p>
 */
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"cloudera", "command execution", "command", "stream", "execute", "kerberos"})
@CapabilityDescription("ExecuteStreamCommand 프로세서는 외부 명령과 스크립트를 NiFi 데이터 흐름에 유연하게 통합할 수 있는 방법을 제공한다."
        + " ExecuteStreamCommand는 파이프(pipe)가 동작하는 방식과 유사하게, 유입된 FlowFile의 콘텐츠를 실행할 명령으로 전달할 수 있다.")
@SupportsSensitiveDynamicProperties
@DynamicProperties({
        @DynamicProperty(name = "환경 변수 이름", value = "환경 변수 값",
                description = "이 환경 변수들은 이 프로세서가 생성(spawn)하는 프로세스에 전달된다"),
        @DynamicProperty(name = "command.argument.<commandIndex>", value = "명령에 전달할 인자",
                description = "Command Arguments Strategy로 Dynamic Property Arguments를 사용할 때, 이 프로세서가 생성하는 프로세스에 "
                        + "전달되는 인자들이다. <commandIndex>는 숫자이며 인자의 순서를 결정한다.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "execution.command", description = "실행된 명령의 이름"),
        @WritesAttribute(attribute = "execution.command.args", description = "세미콜론으로 구분된 인자 목록. 민감한(Sensitive) 속성은 마스킹 처리된다"),
        @WritesAttribute(attribute = "execution.status", description = "명령 실행 후 반환된 종료 상태 코드"),
        @WritesAttribute(attribute = "execution.error", description = "명령 실행 중 반환된 에러 메시지"),
        @WritesAttribute(attribute = "mime.type", description = "'Output MIME Type' 속성이 설정되어 있고 'Output Destination Attribute'가 설정되어 있지 않은 경우, 출력의 MIME 유형을 설정한다")})
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "운영자가 NiFi가 가진 모든 권한을 그대로 사용하여 임의의 코드를 실행할 수 있게 한다.")
        }
)
/**
 * FlowFile 콘텐츠를 외부 셸 명령의 표준 입력(STDIN)으로 전달하고, 그 표준 출력(STDOUT)을
 * 새 FlowFile 콘텐츠(또는 속성)로 캡처하는 프로세서.
 *
 * <p>표준 ExecuteStreamCommand와의 차이점은 Pre-Command / Post-Command 속성을 지원한다는 점이다.
 * 실제 실행 시에는 Pre-Command, (Command Path + Command Arguments로 구성된) 본 명령, Post-Command를
 * 순서대로 하나의 임시 셸 스크립트 파일에 기록한 뒤 {@code /bin/sh <스크립트파일>} 형태로 그 스크립트
 * 전체를 실행한다(자세한 내용은 {@link #onTrigger} 참고).</p>
 */
public class PrePostExecuteStreamCommand extends AbstractProcessor {

    public static final Relationship ORIGINAL_RELATIONSHIP = new Relationship.Builder()
            .name("original")
            .description("원본 FlowFile이 이 관계로 전달된다. 스크립트 실행 결과를 담은 새로운 속성들이 추가된다.")
            .build();
    public static final Relationship OUTPUT_STREAM_RELATIONSHIP = new Relationship.Builder()
            .name("output stream")
            .description("반환된 상태 코드가 0인 경우, 명령의 출력으로 생성된 FlowFile이 전달되는 목적지.")
            .build();
    public static final Relationship NONZERO_STATUS_RELATIONSHIP = new Relationship.Builder()
            .name("nonzero status")
            .description("반환된 상태 코드가 0이 아닌 경우, 명령의 출력으로 생성된 FlowFile이 전달되는 목적지. "
                    + "이 관계로 전달되는 모든 FlowFile은 페널티(penalize)가 적용된다.")
            .build();
    // Command Arguments Strategy에서 선택 가능한 값들. value/displayName은 플로우에 저장되는 식별자이므로 변경하지 않는다.
    static final AllowableValue COMMAND_ARGUMENTS_PROPERTY_STRATEGY = new AllowableValue("Command Arguments Property", "Command Arguments Property",
            "실행 파일에 전달할 인자를 Command Arguments 속성 값에서 가져온다");
    static final AllowableValue DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY = new AllowableValue("Dynamic Property Arguments", "Dynamic Property Arguments",
            "실행 파일에 전달할 인자를 'command.argument.<commandIndex>' 패턴의 동적 속성에서 가져온다");
    static final PropertyDescriptor WORKING_DIR = new PropertyDescriptor.Builder()
            .name("Working Directory")
            .displayName("작업 디렉터리")
            .description("명령 실행 시 현재 작업 디렉토리로 사용할 디렉토리")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, true))
            .required(false)
            .build();
    static final PropertyDescriptor ARGUMENTS_STRATEGY = new PropertyDescriptor.Builder()
            .name("argumentsStrategy")
            .displayName("명령 인자 전략")
            .description("명령에 전달할 인자를 구성하는 전략.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .allowableValues(COMMAND_ARGUMENTS_PROPERTY_STRATEGY, DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY)
            .defaultValue(COMMAND_ARGUMENTS_PROPERTY_STRATEGY.getValue())
            .build();
    static final PropertyDescriptor ARG_DELIMITER = new PropertyDescriptor.Builder()
            .name("Argument Delimiter")
            .displayName("인자 구분자")
            .description("명령의 인자를 구분하는 데 사용할 구분자[기본값: ;]. 반드시 단일 문자여야 한다")
            .dependsOn(ARGUMENTS_STRATEGY, COMMAND_ARGUMENTS_PROPERTY_STRATEGY)
            .addValidator(StandardValidators.SINGLE_CHAR_VALIDATOR)
            .required(true)
            .defaultValue(";")
            .build();
    static final PropertyDescriptor IGNORE_STDIN = new PropertyDescriptor.Builder()
            .name("Ignore STDIN")
            .displayName("STDIN 무시")
            .description("true인 경우, 유입된 FlowFile의 콘텐츠를 실행할 명령에 전달하지 않는다")
            .addValidator(Validator.VALID)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    static final PropertyDescriptor PUT_OUTPUT_IN_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Output Destination Attribute")
            .displayName("출력 대상 속성")
            .description("설정하면, 스트림 명령의 출력이 별도의 FlowFile 대신 원본 FlowFile의 속성에 담긴다. "
                    + "이 경우 'output stream' 또는 'nonzero status' 관계는 더 이상 사용되지 않는다. 이 속성의 값이 출력 속성의 키(key)가 된다.")
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .build();
    static final PropertyDescriptor PUT_ATTRIBUTE_MAX_LENGTH = new PropertyDescriptor.Builder()
            .name("Max Attribute Length")
            .displayName("최대 속성 길이")
            .description("스트림 명령의 출력을 속성으로 라우팅하는 경우, 속성 값에 담을 최대 문자 수. "
                    + "속성은 메모리에 유지되므로 큰 속성 값은 메모리 부족 문제를 빠르게 유발할 수 있어 이 값이 중요하다. "
                    + "출력이 이 값보다 길면 잘라낸다(truncate). 가능하다면 이 값을 더 작게 설정하는 것을 고려하라.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("256")
            .build();
    static final PropertyDescriptor MIME_TYPE = new PropertyDescriptor.Builder()
            .name("Output MIME Type")
            .displayName("출력 MIME 유형")
            .description("\"mime.type\" 속성에 설정할 값을 지정한다. 'Output Destination Attribute'가 설정된 경우 이 속성은 무시된다.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PRE_COMMAND = new PropertyDescriptor.Builder()
            .name("Pre-Command")
            .displayName("사전 명령")
            .description("Command Path 및 Command Arguments의 실행 전에 실행할 커맨드")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor POST_COMMAND = new PropertyDescriptor.Builder()
            .name("Post-Command")
            .displayName("사후 명령")
            .description("Command Path 및 Command Arguments의 실행 후에 실행할 커맨드")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    private final static Set<Relationship> OUTPUT_STREAM_RELATIONSHIP_SET;
    private final static Set<Relationship> ATTRIBUTE_RELATIONSHIP_SET;
    private static final Pattern COMMAND_ARGUMENT_PATTERN = Pattern.compile("command\\.argument\\.(?<commandIndex>[0-9]+)$");
    private static final Validator ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR = StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true);
    static final PropertyDescriptor EXECUTION_COMMAND = new PropertyDescriptor.Builder()
            .name("Command Path")
            .displayName("명령 경로")
            .description("실행할 명령을 지정한다; 실행 파일 이름만 지정하는 경우 사용자 환경의 PATH에 존재해야 한다.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor EXECUTION_ARGUMENTS = new PropertyDescriptor.Builder()
            .name("Command Arguments")
            .displayName("명령 인자")
            .description("실행 파일에 전달할 인자들을 ';' 문자로 구분하여 지정한다.")
            .dependsOn(ARGUMENTS_STRATEGY, COMMAND_ARGUMENTS_PROPERTY_STRATEGY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator((subject, input, context) -> {
                ValidationResult result = new ValidationResult.Builder()
                        .subject(subject).valid(true).input(input).build();
                List<String> args = ArgumentUtils.splitArgs(input, context.getProperty(PrePostExecuteStreamCommand.ARG_DELIMITER).getValue().charAt(0));
                for (String arg : args) {
                    ValidationResult valResult = ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR.validate(subject, arg, context);
                    if (!valResult.isValid()) {
                        result = valResult;
                        break;
                    }
                }
                return result;
            }).build();
    private static final List<PropertyDescriptor> PROPERTIES;
    private static final String MASKED_ARGUMENT = "********";

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PRE_COMMAND);
        props.add(POST_COMMAND);
        // 참고: WORKING_DIR이 중복으로 두 번 추가되어 있다(기존 동작 유지를 위해 그대로 둠).
        props.add(WORKING_DIR);
        props.add(WORKING_DIR);
        props.add(EXECUTION_COMMAND);
        props.add(ARGUMENTS_STRATEGY);
        props.add(EXECUTION_ARGUMENTS);
        props.add(ARG_DELIMITER);
        props.add(IGNORE_STDIN);
        props.add(PUT_OUTPUT_IN_ATTRIBUTE);
        props.add(PUT_ATTRIBUTE_MAX_LENGTH);
        props.add(MIME_TYPE);
        PROPERTIES = Collections.unmodifiableList(props);

        Set<Relationship> outputStreamRelationships = new HashSet<>();
        outputStreamRelationships.add(OUTPUT_STREAM_RELATIONSHIP);
        outputStreamRelationships.add(ORIGINAL_RELATIONSHIP);
        outputStreamRelationships.add(NONZERO_STATUS_RELATIONSHIP);
        OUTPUT_STREAM_RELATIONSHIP_SET = Collections.unmodifiableSet(outputStreamRelationships);

        Set<Relationship> attributeRelationships = new HashSet<>();
        attributeRelationships.add(ORIGINAL_RELATIONSHIP);
        ATTRIBUTE_RELATIONSHIP_SET = Collections.unmodifiableSet(attributeRelationships);
    }

    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();
    private ComponentLog logger;

    /**
     * FlowFile의 콘텐츠(incomingFlowFileIS)를 별도의 데몬 스레드에서 프로세스의 표준 입력(stdinWritable)으로
     * 복사하는 작업을 시작한다. 별도 스레드로 처리하는 이유는, 프로세스의 STDOUT을 읽는 동작과 STDIN에 쓰는
     * 동작을 동시에 수행하지 않으면(즉 한쪽만 블로킹 방식으로 처리하면) 프로세스가 STDOUT 버퍼를 가득 채운 채
     * STDIN을 기다리는 상황에서 교착 상태(deadlock)에 빠질 수 있기 때문이다.
     * ignoreStdin이 true이면 실제로 아무 데이터도 쓰지 않고 즉시 스트림을 닫아 프로세스에 입력이 없음을 알린다.
     */
    private static void readStdoutReadable(final boolean ignoreStdin, final OutputStream stdinWritable,
                                           final ComponentLog logger, final InputStream incomingFlowFileIS) {
        Thread writerThread = new Thread(() -> {
            if (!ignoreStdin) {
                try {
                    StreamUtils.copy(incomingFlowFileIS, stdinWritable);
                } catch (IOException e) {
                    // This is unlikely to occur, and isn't handled at the moment
                    // Bug captured in NIFI-1194
                    logger.error("Failed to write FlowFile to Standard Input Stream", e);
                }
            }
            // MUST close the output stream to the stdin so that whatever is reading knows
            // there is no more data.
            // 반드시 stdin 쪽 출력 스트림을 닫아야, 이를 읽는 프로세스가 더 이상 데이터가 없음을 알 수 있다.
            IOUtils.closeQuietly(stdinWritable);
        });
        writerThread.setDaemon(true);
        writerThread.start();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        logger = getLogger();

        relationships.set(OUTPUT_STREAM_RELATIONSHIP_SET);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(PUT_OUTPUT_IN_ATTRIBUTE)) {
            if (newValue != null) {
                relationships.set(ATTRIBUTE_RELATIONSHIP_SET);
            } else {
                relationships.set(OUTPUT_STREAM_RELATIONSHIP_SET);
            }
        }
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    /**
     * 사용자가 정의한 동적 속성의 이름에 따라 그 의미를 두 가지로 해석한다:
     * "command.argument.&lt;숫자&gt;" 패턴과 일치하면 명령에 전달할 인자로, 그렇지 않으면
     * 실행될 프로세스의 환경 변수로 취급한다.
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final Matcher matcher = COMMAND_ARGUMENT_PATTERN.matcher(propertyDescriptorName);
        if (matcher.matches()) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(propertyDescriptorName)
                    .description("명령에 전달되는 인자")
                    .dynamic(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .addValidator(ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
                    .build();
        } else {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .description("프로세스의 환경 변수 '" + propertyDescriptorName + "'를 설정한다")
                    .dynamic(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();
        }
    }

    /**
     * FlowFile 하나를 받아 다음 순서로 처리한다.
     * <ol>
     *   <li>Command Arguments Strategy(속성 방식 vs 동적 속성 방식)에 따라 명령 인자 목록을 구성한다.</li>
     *   <li>Pre-Command, 본 명령(Command Path + 인자), Post-Command를 순서대로 하나의 임시 셸 스크립트 파일에 기록한다.</li>
     *   <li>{@code /bin/sh <스크립트파일>} 형태로 프로세스를 실행하고, FlowFile 콘텐츠를 STDIN으로 스트리밍하면서
     *       STDOUT을 새 FlowFile(또는 속성)로 캡처한다({@link ProcessStreamWriterCallback} 참고).</li>
     *   <li>종료 코드(exit code)와 STDERR 내용을 속성으로 기록하고, 종료 코드에 따라 성공/실패 관계로 라우팅한다.</li>
     *   <li>임시 파일(셸 스크립트, STDERR 캡처 파일)을 정리하고 프로세스를 종료(destroy)한다.</li>
     * </ol>
     */
    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = session.get();
        if (null == inputFlowFile) {
            return;
        }

        final ArrayList<String> args = new ArrayList<>(); // Commandline Args
        final ArrayList<String> argumentAttributeValue = new ArrayList<>();
        final boolean putToAttribute = context.getProperty(PUT_OUTPUT_IN_ATTRIBUTE).isSet();
        final PropertyValue argumentsStrategyPropertyValue = context.getProperty(ARGUMENTS_STRATEGY);
        final boolean useDynamicPropertyArguments = argumentsStrategyPropertyValue.isSet() && argumentsStrategyPropertyValue.getValue().equals(DYNAMIC_PROPERTY_ARGUMENTS_STRATEGY.getValue());
        final Integer attributeSize = context.getProperty(PUT_ATTRIBUTE_MAX_LENGTH).asInteger();
        final String attributeName = context.getProperty(PUT_OUTPUT_IN_ATTRIBUTE).getValue();

        final String workingDir = context.getProperty(WORKING_DIR).evaluateAttributeExpressions(inputFlowFile).getValue();

        ////////////////////////////////////
        /// PRE-POST COMMAND START
        ////////////////////////////////////
        // Pre-Command / Post-Command 값을 평가하고, 이후 이 값들과 본 명령을 하나로 묶어 기록할
        // 임시 셸 스크립트 파일을 준비한다.

        final String preCommand = context.getProperty(PRE_COMMAND).evaluateAttributeExpressions(inputFlowFile).getValue();
        final String postCommand = context.getProperty(POST_COMMAND).evaluateAttributeExpressions(inputFlowFile).getValue();

        final File shellFilename;
        try {
            shellFilename = File.createTempFile("nifi", UUID.randomUUID().toString() + ".sh");
        } catch (IOException e) {
            logger.warn("Could not write shell script to temporary. Cause: {}", e.getMessage(), e);
            session.rollback(true);
            return;
        }

        ////////////////////////////////////
        /// PRE-POST COMMAND END
        ////////////////////////////////////

        final String executeCommand = context.getProperty(EXECUTION_COMMAND).evaluateAttributeExpressions(inputFlowFile).getValue(); // Command for executing
        args.add(executeCommand);
        final boolean ignoreStdin = Boolean.parseBoolean(context.getProperty(IGNORE_STDIN).getValue());
        final String commandArguments;
        if (!useDynamicPropertyArguments) {
            // Command Arguments Property 전략: 지정된 구분자로 인자 문자열을 분리한다(따옴표 미지원).
            commandArguments = context.getProperty(EXECUTION_ARGUMENTS).evaluateAttributeExpressions(inputFlowFile).getValue();
            if (!StringUtils.isBlank(commandArguments)) {
                args.addAll(ArgumentUtils.splitArgs(commandArguments, context.getProperty(ARG_DELIMITER).getValue().charAt(0)));
            }
        } else {
            // Dynamic Property Arguments 전략: "command.argument.<숫자>" 패턴의 동적 속성들을 모두 찾아
            // <숫자> 순서대로 정렬한 뒤, 각 값을 순서대로 하나의 인자로 추가한다.
            List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();
            for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                Matcher matcher = COMMAND_ARGUMENT_PATTERN.matcher(entry.getKey().getName());
                if (matcher.matches()) {
                    propertyDescriptors.add(entry.getKey());
                }
            }
            // commandIndex(숫자) 오름차순으로 정렬하여 인자 순서를 결정한다.
            propertyDescriptors.sort((p1, p2) -> {
                Matcher matcher = COMMAND_ARGUMENT_PATTERN.matcher(p1.getName());
                String indexString1 = null;
                while (matcher.find()) {
                    indexString1 = matcher.group("commandIndex");
                }
                matcher = COMMAND_ARGUMENT_PATTERN.matcher(p2.getName());
                String indexString2 = null;
                while (matcher.find()) {
                    indexString2 = matcher.group("commandIndex");
                }
                final int index1 = Integer.parseInt(indexString1);
                final int index2 = Integer.parseInt(indexString2);
                if (index1 > index2) {
                    return 1;
                } else if (index1 < index2) {
                    return -1;
                }
                return 0;
            });

            for (final PropertyDescriptor descriptor : propertyDescriptors) {
                String argValue = context.getProperty(descriptor.getName()).evaluateAttributeExpressions(inputFlowFile).getValue();
                if (descriptor.isSensitive()) {
                    // 민감한(Sensitive) 속성값은 execution.command.args 속성에 원본 값 대신 마스킹 문자열을 남긴다.
                    argumentAttributeValue.add(MASKED_ARGUMENT);
                } else {
                    argumentAttributeValue.add(argValue);
                }
                args.add(argValue);

            }
            if (argumentAttributeValue.size() > 0) {
                final StringBuilder builder = new StringBuilder();
                for (String s : argumentAttributeValue) {
                    builder.append(s).append("\t");
                }
                commandArguments = builder.toString().trim();
            } else {
                commandArguments = "";
            }
        }

        // Pre-Command, 본 명령(Command Path + 인자를 공백으로 이어붙인 문자열), Post-Command를
        // 순서대로 하나의 명령 스택으로 구성한다. 비어있는 Pre/Post 명령은 생략한다.
        List<String> commandStack = new ArrayList<>();
        if (StringUtils.isNotBlank(preCommand)) {
            commandStack.add(preCommand);
        }
        commandStack.add(String.join(" ", args));
        if (StringUtils.isNotBlank(postCommand)) {
            commandStack.add(postCommand);
        }

        // 구성된 명령 스택을 줄바꿈으로 이어붙여 임시 셸 스크립트 파일에 UTF-8로 기록한다.
        // 이후 이 스크립트 파일 자체를 /bin/sh로 실행하여 Pre/본/Post 명령이 순차적으로 수행되게 한다.
        try {
            IOUtils.write(String.join("\n", commandStack), new FileOutputStream(shellFilename), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.warn("Could not write shell script to {}.  Command will not be executed", shellFilename.getAbsolutePath(), e);
            session.rollback(true);
            return;
        }

        // 실제 실행할 프로세스 명령을 "/bin/sh <스크립트파일>"로 재구성한다.
        args.clear();
        args.add("/bin/sh");
        args.add(shellFilename.getAbsolutePath());

        logger.info("Executing and waiting for command: {}\n-----------------------------\n{}\n-----------------------------", String.join(" ", args), String.join("\n", commandStack));

        final ProcessBuilder builder = new ProcessBuilder();

        // Avoid logging arguments that could contain sensitive values
        // 작업 디렉토리가 지정되어 있으면 사용하고, 존재하지 않으면 생성을 시도한다(실패해도 처리는 계속 진행).
        File dir = null;
        if (!StringUtils.isBlank(workingDir)) {
            dir = new File(workingDir);
            if (!dir.exists() && !dir.mkdirs()) {
                logger.warn("Failed to create working directory {}, using current working directory {}", workingDir, System.getProperty("user.dir"));
            }
        }
        // 동적 속성 중 "command.argument.*" 패턴이 아닌 나머지는 모두 프로세스의 환경 변수로 전달한다.
        final Map<String, String> environment = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                environment.put(entry.getKey().getName(), entry.getValue());
            }
        }
        builder.environment().putAll(environment);
        builder.command(args); // Commandline + All Args
        builder.directory(dir);
        builder.redirectInput(Redirect.PIPE);
        builder.redirectOutput(Redirect.PIPE);
        // 표준 에러(STDERR)는 파이프 대신 임시 파일로 직접 리다이렉트한다(메모리 사용량 절감 및 데드락 회피).
        final File errorOut;
        try {
            errorOut = File.createTempFile("out", null);
            builder.redirectError(errorOut);
        } catch (IOException e) {
            if (shellFilename.exists()) {
                shellFilename.delete();
            }

            logger.error("Could not create temporary file for error logging", e);
            throw new ProcessException(e);
        }

        final Process process;
        try {
            process = builder.start(); // Run Command
        } catch (IOException e) {
            if (shellFilename.exists()) {
                shellFilename.delete();
            }

            try {
                if (!errorOut.delete()) {
                    logger.warn("Unable to delete file: " + errorOut.getAbsolutePath());
                }
            } catch (SecurityException se) {
                logger.warn("Unable to delete file: '" + errorOut.getAbsolutePath() + "' due to " + se);
            }
            logger.error("Could not create external process to run command", e);
            throw new ProcessException(e);
        }
        // 프로세스의 STDIN(pos)/STDOUT(pis)에 연결된 파이프 스트림을 사용하여 FlowFile 콘텐츠를 주고받는다.
        // outputFlowFile은 putToAttribute 옵션에 따라 원본 FlowFile을 그대로 쓰거나(속성에 기록하는 경우)
        // 새 FlowFile을 생성한다(별도 콘텐츠로 출력하는 경우).
        try (final OutputStream pos = process.getOutputStream();
             final InputStream pis = process.getInputStream();
             final BufferedInputStream bis = new BufferedInputStream(pis)) {
            final BufferedOutputStream bos = new BufferedOutputStream(pos);
            FlowFile outputFlowFile = putToAttribute ? inputFlowFile : session.create(inputFlowFile);

            // ProcessStreamWriterCallback이 실제로 STDIN 쓰기/STDOUT 읽기를 수행하고 프로세스 종료를 대기한다.
            ProcessStreamWriterCallback callback = new ProcessStreamWriterCallback(ignoreStdin, bos, bis, logger,
                    attributeName, session, outputFlowFile, process, putToAttribute, attributeSize);
            session.read(inputFlowFile, callback);

            outputFlowFile = callback.outputFlowFile;
            if (putToAttribute) {
                outputFlowFile = session.putAttribute(outputFlowFile, attributeName, new String(callback.outputBuffer, 0, callback.size));
            }

            int exitCode = callback.exitCode;
            logger.info(String.format("Execution complete for command: %s.  Exited with code: %s\n-----------------------------\n%s\n-----------------------------", String.join(" ", args), exitCode, String.join("\n", commandStack)));

            Map<String, String> attributes = new HashMap<>();

            // 프로세스 실행 중 STDERR로 출력된 내용을 임시 파일(errorOut)에서 읽어와 최대 4000자까지만
            // execution.error 속성에 담는다(속성이 메모리에 유지되므로 크기를 제한).
            final StringBuilder strBldr = new StringBuilder();
            try (final InputStream is = new FileInputStream(errorOut)) {
                int c;
                while ((c = is.read()) != -1) {
                    strBldr.append((char) c);
                }
            } catch (IOException e) {
                strBldr.append("Unknown...could not read Process's Std Error");
            }
            int length = Math.min(strBldr.length(), 4000);
            attributes.put("execution.error", strBldr.substring(0, length));

            final Relationship outputFlowFileRelationship = putToAttribute ? ORIGINAL_RELATIONSHIP : (exitCode != 0) ? NONZERO_STATUS_RELATIONSHIP : OUTPUT_STREAM_RELATIONSHIP;
            if (exitCode == 0) {
                logger.info("Transferring {} to {}", outputFlowFile, outputFlowFileRelationship.getName());
            } else {
                logger.error("Transferring {} to {}. Executable command {} ended in an error: {}",
                        outputFlowFile, outputFlowFileRelationship.getName(), executeCommand, strBldr.toString());
            }

            attributes.put("execution.status", Integer.toString(exitCode));
            attributes.put("execution.script", String.join("\n", commandStack));
            attributes.put("execution.command", executeCommand);
            attributes.put("execution.command.args", commandArguments);
            if (context.getProperty(MIME_TYPE).isSet() && !putToAttribute) {
                attributes.put(CoreAttributes.MIME_TYPE.key(), context.getProperty(MIME_TYPE).getValue());
            }
            outputFlowFile = session.putAllAttributes(outputFlowFile, attributes);

            if (NONZERO_STATUS_RELATIONSHIP.equals(outputFlowFileRelationship)) {
                outputFlowFile = session.penalize(outputFlowFile);
            }
            // This will transfer the FlowFile that received the stream output to its destined relationship.
            // In the event the stream is put to an attribute of the original, it will be transferred here.
            session.transfer(outputFlowFile, outputFlowFileRelationship);

            if (!putToAttribute) {
                logger.info("Transferring {} to original", inputFlowFile);
                inputFlowFile = session.putAllAttributes(inputFlowFile, attributes);
                session.transfer(inputFlowFile, ORIGINAL_RELATIONSHIP);
            }

        } catch (final IOException e) {
            // could not close Process related streams
            logger.warn("Problem terminating Process {}", process, e);
        } finally {
            // 임시로 생성한 셸 스크립트 파일과 STDERR 캡처 파일을 정리하고, 혹시 남아있을 프로세스를 강제 종료한다.
            if (shellFilename.exists()) {
                shellFilename.delete();
            }
            FileUtils.deleteQuietly(errorOut);
            process.destroy(); // last ditch effort to clean up that process.
        }
    }

    /**
     * FlowFile 콘텐츠를 프로세스의 STDIN으로 전달하고 프로세스의 STDOUT을 읽어 출력 FlowFile(또는
     * 지정된 크기로 제한된 내부 버퍼)에 담는 콜백. STDIN 쓰기는 {@link #readStdoutReadable}을 통해
     * 별도 스레드에서 비동기로 수행되며, 이 클래스는 그동안 STDOUT을 동기적으로 읽어 교착 상태를 방지한다.
     * 두 가지 출력 모드를 지원한다:
     * <ul>
     *   <li>putToAttribute=true: STDOUT을 {@link SoftLimitBoundedByteArrayOutputStream}으로 제한된 크기까지만 읽어
     *       버퍼에 담는다(추후 attributeName 속성 값으로 사용됨).</li>
     *   <li>putToAttribute=false: STDOUT을 그대로 outputFlowFile의 새 콘텐츠로 기록한다.</li>
     * </ul>
     */
    static class ProcessStreamWriterCallback implements InputStreamCallback {

        final boolean ignoreStdin;
        final OutputStream stdinWritable;
        final InputStream stdoutReadable;
        final ComponentLog logger;
        final ProcessSession session;
        final Process process;
        final boolean putToAttribute;
        final int attributeSize;
        final String attributeName;
        FlowFile outputFlowFile;
        int exitCode;
        byte[] outputBuffer;
        int size;

        public ProcessStreamWriterCallback(boolean ignoreStdin, OutputStream stdinWritable, InputStream stdoutReadable, ComponentLog logger, String attributeName,
                                           ProcessSession session, FlowFile outputFlowFile, Process process, boolean putToAttribute, int attributeSize) {
            this.ignoreStdin = ignoreStdin;
            this.stdinWritable = stdinWritable;
            this.stdoutReadable = stdoutReadable;
            this.logger = logger;
            this.session = session;
            this.outputFlowFile = outputFlowFile;
            this.process = process;
            this.putToAttribute = putToAttribute;
            this.attributeSize = attributeSize;
            this.attributeName = attributeName;
        }

        /**
         * NiFi 세션 콜백 진입점. 먼저 STDIN 쓰기 스레드를 시작시킨 뒤(비동기), 현재 스레드에서는
         * 프로세스의 STDOUT을 동기적으로 소비하고, 다 읽은 뒤 프로세스 종료를 대기(waitFor)한다.
         * STDIN 쓰기와 STDOUT 읽기를 병행해야 프로세스의 출력 버퍼가 가득 차 STDIN 쓰기가 막히는
         * 교착 상태를 피할 수 있다.
         */
        @Override
        public void process(final InputStream incomingFlowFileIS) throws IOException {
            if (putToAttribute) {
                // 출력 크기를 attributeSize로 소프트 제한하는 버퍼에 STDOUT을 담는다(속성으로 사용하기 위함).
                try (SoftLimitBoundedByteArrayOutputStream softLimitBoundedBAOS = new SoftLimitBoundedByteArrayOutputStream(attributeSize)) {
                    readStdoutReadable(ignoreStdin, stdinWritable, logger, incomingFlowFileIS);
                    final long longSize = StreamUtils.copy(stdoutReadable, softLimitBoundedBAOS);

                    // Because the outputStream has a cap that the copy doesn't know about, adjust
                    // the actual size
                    // StreamUtils.copy는 상한(cap)을 모르고 실제 읽은 바이트 수를 반환하므로,
                    // 버퍼에 실제로 담긴 크기(size)는 attributeSize를 넘지 않도록 별도로 보정한다.
                    if (longSize > attributeSize) { // Explicit cast for readability
                        size = attributeSize;
                    } else {
                        size = (int) longSize; // Note: safe cast, longSize is limited by attributeSize
                    }

                    outputBuffer = softLimitBoundedBAOS.getBuffer();
                    stdoutReadable.close();

                    try {
                        exitCode = process.waitFor();
                    } catch (InterruptedException e) {
                        logger.warn("Command Execution Process was interrupted", e);
                    }
                }
            } else {
                // STDOUT을 그대로 outputFlowFile의 새 콘텐츠로 스트리밍하여 기록한다.
                outputFlowFile = session.write(outputFlowFile, out -> {
                    readStdoutReadable(ignoreStdin, stdinWritable, logger, incomingFlowFileIS);
                    StreamUtils.copy(stdoutReadable, out);
                    try {
                        exitCode = process.waitFor();
                    } catch (InterruptedException e) {
                        logger.warn("Command Execution Process was interrupted", e);
                    }
                });
            }
        }
    }
}

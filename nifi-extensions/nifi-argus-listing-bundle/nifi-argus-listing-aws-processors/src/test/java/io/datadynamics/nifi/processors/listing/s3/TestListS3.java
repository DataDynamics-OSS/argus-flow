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
package io.datadynamics.nifi.processors.listing.s3;

import io.datadynamics.nifi.processor.util.list.TriggeredListing;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.AbstractAwsProcessor;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestListS3 {

    private S3Client mockS3Client;
    private final Map<String, String> regionsRequested = new ConcurrentHashMap<>();
    private TestRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        mockS3Client = mock(S3Client.class);
        final ListS3 processor = new ListS3() {
            @Override
            protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
                regionsRequested.put(attributes.getOrDefault("request.id", ""), RegionUtil.getRegion(context, attributes).id());
                return mockS3Client;
            }

            @Override
            protected S3Client createClient(final ProcessContext context, final Map<String, String> attributes) {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(processor);

        final MockCredentialsService credentials = new MockCredentialsService();
        runner.addControllerService("creds", credentials);
        runner.enableControllerService(credentials);
        runner.setProperty(AbstractAwsProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE, "creds");
        runner.setProperty(RegionUtil.REGION, "eu-west-1");
        runner.setProperty(ListS3.BUCKET_WITHOUT_DEFAULT_VALUE, "${s3.bucket}");
        runner.setProperty(ListS3.PREFIX, "${s3.prefix}");
    }

    private static ListObjectsResponse objects(final String... keys) {
        final Instant lastModified = Instant.now().minusSeconds(60);
        return ListObjectsResponse.builder()
                .contents(Arrays.stream(keys).map(key -> S3Object.builder().key(key).lastModified(lastModified).size(1L).build()).toList())
                .isTruncated(false)
                .build();
    }

    @Test
    void listsBucketAndPrefixNamedByFlowFile() {
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(objects("in/a", "in/b"));
        runner.enqueue(new byte[0], Map.of("s3.bucket", "bucket-1", "s3.prefix", "in/", "request.id", "r1"));

        runner.run();

        final ArgumentCaptor<ListObjectsRequest> request = ArgumentCaptor.forClass(ListObjectsRequest.class);
        verify(mockS3Client, times(1)).listObjects(request.capture());
        assertEquals("bucket-1", request.getValue().bucket());
        assertEquals("in/", request.getValue().prefix());

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 2);
        for (final MockFlowFile child : runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS)) {
            child.assertAttributeEquals("s3.bucket", "bucket-1");
            child.assertAttributeEquals("request.id", "r1");
            child.assertAttributeEquals(TriggeredListing.ATTR_TARGET, "S3 [s3://bucket-1/in/] in [eu-west-1]");
        }
        assertEquals(List.of("in/a", "in/b"), runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).stream()
                .map(f -> f.getAttribute("filename")).sorted().toList());
        runner.getFlowFilesForRelationship(TriggeredListing.REL_ORIGINAL).getFirst().assertAttributeEquals(TriggeredListing.ATTR_COUNT, "2");
    }

    @Test
    void customRegionIsEvaluatedAgainstFlowFile() {
        runner.setProperty(RegionUtil.REGION, RegionUtil.USE_CUSTOM_REGION.getValue());
        runner.setProperty(RegionUtil.CUSTOM_REGION_WITH_FF_EL, "${s3.region}");
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(objects("a"));
        runner.enqueue(new byte[0], Map.of("s3.bucket", "bucket-1", "s3.region", "ap-northeast-2", "request.id", "r1"));

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).getFirst().assertAttributeEquals("s3.region", "ap-northeast-2");
        assertEquals("ap-northeast-2", regionsRequested.get("r1"));
    }

    @Test
    void trackingTimestampsListsOnlyNewObjects() {
        runner.setProperty(TriggeredListing.LISTING_STRATEGY, TriggeredListing.BY_TIMESTAMPS);
        final Instant first = Instant.ofEpochMilli(1_000_000L);
        final Instant second = Instant.ofEpochMilli(2_000_000L);
        final S3Object a = S3Object.builder().key("a").lastModified(first).size(1L).build();
        final S3Object b = S3Object.builder().key("b").lastModified(second).size(1L).build();
        when(mockS3Client.listObjects(any(ListObjectsRequest.class)))
                .thenReturn(ListObjectsResponse.builder().contents(a).isTruncated(false).build())
                .thenReturn(ListObjectsResponse.builder().contents(a, b).isTruncated(false).build());

        runner.enqueue(new byte[0], Map.of("s3.bucket", "bucket-1"));
        runner.run();
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);

        runner.clearTransferState();
        runner.enqueue(new byte[0], Map.of("s3.bucket", "bucket-1"));
        runner.run();
        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).getFirst().assertAttributeEquals("filename", "b");
    }

    @Test
    void bucketOutsideAllowedBucketsIsRejectedBeforeListing() {
        runner.setProperty(ListS3.ALLOWED_BUCKETS, "landing-.*");
        runner.enqueue(new byte[0], Map.of("s3.bucket", "secret-bucket"));

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
        verify(mockS3Client, never()).listObjects(any(ListObjectsRequest.class));
    }

    @Test
    void sdkFailureRoutesFailure() {
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenThrow(SdkClientException.create("connection refused"));
        runner.enqueue(new byte[0], Map.of("s3.bucket", "bucket-1"));

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 0);
        runner.assertTransferCount(TriggeredListing.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_FAILURE).getFirst()
                .assertAttributeEquals(TriggeredListing.ATTR_ERROR_TYPE, SdkClientException.class.getName());
    }

    @Test
    void recordWriterWritesOneChildPerInput() throws Exception {
        final MockRecordWriter writer = new MockRecordWriter("key");
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(TriggeredListing.RECORD_WRITER, "writer");
        when(mockS3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(objects("a", "b", "c"));
        runner.enqueue(new byte[0], Map.of("s3.bucket", "bucket-1"));

        runner.run();

        runner.assertTransferCount(TriggeredListing.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(TriggeredListing.REL_SUCCESS).getFirst().assertAttributeEquals("record.count", "3");
    }

    private static class MockCredentialsService extends AbstractControllerService implements AwsCredentialsProviderService {
        @Override
        public AwsCredentialsProvider getAwsCredentialsProvider() {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"));
        }
    }
}

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
 *   nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/util/SoftLimitBoundedByteArrayOutputStream.java
 */
package io.datadynamics.nifi.processors.command;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 지정된 한도(limit)까지만 기록을 허용하는 바이트 배열 출력 스트림.
 * 한도를 초과하는 쓰기 요청은 예외를 던지지 않고 조용히 무시(silent truncation)되며,
 * 이를 통해 스트림 내용을 정해진 크기의 배열에 맞게 자연스럽게 잘라낼 수 있다.
 */
public class SoftLimitBoundedByteArrayOutputStream extends OutputStream {
    /*
     * This Bounded Array Output Stream (BAOS) allows the user to write to the output stream up to a specified limit.
     * Higher than that limit the BAOS will silently return and not put more into the buffer. It also will not throw an error.
     * This effectively truncates the stream for the user to fit into a bounded array.
     */

    // 실제 데이터를 저장하는 내부 버퍼(용량은 capacity로 고정)
    private final byte[] buffer;
    // 현재 쓰기가 허용되는 한도(버퍼 용량 이하의 값). 한도 도달 시 이후 쓰기는 무시된다.
    private int limit;
    // 현재까지 버퍼에 기록된 바이트 수
    private int count;

    /**
     * 버퍼 용량과 쓰기 한도를 동일하게 설정하여 생성한다.
     */
    public SoftLimitBoundedByteArrayOutputStream(int capacity) {
        this(capacity, capacity);
    }

    /**
     * 버퍼 용량(capacity)과 실제 쓰기 한도(limit)를 각각 지정하여 생성한다.
     * limit은 capacity보다 클 수 없다.
     */
    public SoftLimitBoundedByteArrayOutputStream(int capacity, int limit) {
        if ((capacity < limit) || (capacity | limit) < 0) {
            throw new IllegalArgumentException("Invalid capacity/limit");
        }
        this.buffer = new byte[capacity];
        this.limit = limit;
        this.count = 0;
    }

    /**
     * 바이트 하나를 기록한다. 한도에 도달한 경우 예외 없이 아무 동작도 하지 않는다.
     */
    @Override
    public void write(int b) throws IOException {
        if (count >= limit) {
            return;
        }
        buffer[count++] = (byte) b;
    }

    /**
     * 바이트 배열의 일부(off부터 len개)를 기록한다.
     * 남은 한도보다 len이 크면 한도에 맞게 잘라서(truncate) 기록하고,
     * 이미 한도에 도달한 상태라면 아무 것도 기록하지 않는다.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
                || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        // 남은 한도를 초과하는 만큼은 잘라내어 실제로 기록할 길이를 재조정한다.
        if (count + len > limit) {
            len = limit - count;
            if (len == 0) {
                return;
            }
        }

        System.arraycopy(b, off, buffer, count, len);
        count += len;
    }

    /**
     * 쓰기 한도를 newlim으로 재설정하고 기록된 데이터 개수(count)를 0으로 초기화한다.
     * newlim은 버퍼 용량을 초과할 수 없다.
     */
    public void reset(int newlim) {
        if (newlim > buffer.length) {
            throw new IndexOutOfBoundsException("Limit exceeds buffer size");
        }
        this.limit = newlim;
        this.count = 0;
    }

    /**
     * 쓰기 한도를 버퍼 전체 용량으로 되돌리고 기록된 데이터 개수를 0으로 초기화한다.
     */
    public void reset() {
        this.limit = buffer.length;
        this.count = 0;
    }

    public int getLimit() {
        return limit;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int size() {
        return count;
    }
}

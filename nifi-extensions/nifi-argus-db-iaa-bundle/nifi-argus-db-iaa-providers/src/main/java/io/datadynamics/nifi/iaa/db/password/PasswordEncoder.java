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
package io.datadynamics.nifi.iaa.db.password;

/**
 * 비밀번호 해싱. NiFi single-user 프로바이더의 동명 인터페이스와 같은 계약이며, 해시 형식도
 * 같아서(bcrypt {@code $2b$12$}) 기존 자격증명을 그대로 이관할 수 있다.
 */
public interface PasswordEncoder {

    /** 평문을 해싱한다. */
    String encode(char[] password);

    /**
     * 평문이 해시와 일치하는지 검사한다. 구현은 상수시간 비교를 사용해야 한다.
     *
     * @return 해시 형식이 잘못된 경우에도 예외 대신 {@code false}
     */
    boolean matches(char[] password, String hash);
}

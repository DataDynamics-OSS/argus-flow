-- Copyright 2026 Data Dynamics Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Argus Flow — DB 기반 인증·인가 스키마 v1 (H2)
--
-- **테스트 전용이다.** 운영 배포에는 postgresql/ 또는 mariadb/ 판을 쓴다.
-- 단위 테스트가 인메모리 H2 에 이 파일을 적용해 DAO 가 기대하는 컬럼을 검증한다.
--
-- PostgreSQL 판과 거의 같다. 차이는 MERGE INTO 뿐 — H2 는 ON CONFLICT 를
-- 지원하지 않는다.

CREATE TABLE IF NOT EXISTS argus_user (
    id            VARCHAR(36)  NOT NULL,
    identity      VARCHAR(255) NOT NULL,
    password_hash VARCHAR(100),
    enabled       BOOLEAN      NOT NULL DEFAULT TRUE,
    failed_count  INTEGER      NOT NULL DEFAULT 0,
    locked_until  TIMESTAMP,
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_argus_user PRIMARY KEY (id),
    CONSTRAINT uq_argus_user_identity UNIQUE (identity)
);

CREATE TABLE IF NOT EXISTS argus_group (
    id         VARCHAR(36)  NOT NULL,
    name       VARCHAR(255) NOT NULL,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_argus_group PRIMARY KEY (id),
    CONSTRAINT uq_argus_group_name UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS argus_user_group (
    user_id  VARCHAR(36) NOT NULL,
    group_id VARCHAR(36) NOT NULL,
    CONSTRAINT pk_argus_user_group PRIMARY KEY (user_id, group_id),
    CONSTRAINT fk_argus_user_group_user
        FOREIGN KEY (user_id) REFERENCES argus_user (id) ON DELETE CASCADE,
    CONSTRAINT fk_argus_user_group_group
        FOREIGN KEY (group_id) REFERENCES argus_group (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_argus_user_group_group ON argus_user_group (group_id);

CREATE TABLE IF NOT EXISTS argus_schema_version (
    version    INTEGER   NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_argus_schema_version PRIMARY KEY (version)
);

MERGE INTO argus_schema_version (version) KEY (version) VALUES (1);

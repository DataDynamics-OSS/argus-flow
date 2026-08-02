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

-- Argus Flow — DB 기반 인증·인가 스키마 v1 (MariaDB)
--
-- 적용:  bin/argus-user.sh schema-init
--        또는 mariadb < V1__baseline.sql
--
-- PostgreSQL 판과의 차이:
--   * ENGINE=InnoDB       — FOREIGN KEY 와 ON DELETE CASCADE 에 필요
--   * utf8mb4_bin         — identity 대소문자를 구분한다. 기본 collation 은 대소문자를
--                           구분하지 않아 'Alice' 와 'alice' 가 같은 사용자가 된다
--   * DATETIME            — TIMESTAMP 는 첫 컬럼에 암묵적 ON UPDATE CURRENT_TIMESTAMP 가
--                           붙어 updated_at 이 의도치 않게 갱신된다
--   * CREATE INDEX 를 테이블 정의 안에 둔다 — MariaDB 는 CREATE INDEX IF NOT EXISTS 를
--                           지원하지 않는 버전이 있다

CREATE TABLE IF NOT EXISTS argus_user (
    id            VARCHAR(36)  NOT NULL,
    identity      VARCHAR(255) NOT NULL,
    -- bcrypt 해시($2b$12$...). NULL 이면 비밀번호 인증 불가 —
    -- 인증서·OIDC 로 인증하고 인가만 DB 로 관리하는 사용자를 위한 것.
    password_hash VARCHAR(100),
    enabled       BOOLEAN      NOT NULL DEFAULT TRUE,
    -- 연속 로그인 실패 횟수와 잠금 해제 시각. 클러스터 전 노드가 상태를 공유해야
    -- 하므로 노드 메모리가 아니라 DB 에 둔다.
    failed_count  INTEGER      NOT NULL DEFAULT 0,
    locked_until  DATETIME,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_argus_user PRIMARY KEY (id),
    CONSTRAINT uq_argus_user_identity UNIQUE (identity)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS argus_group (
    id         VARCHAR(36)  NOT NULL,
    name       VARCHAR(255) NOT NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_argus_group PRIMARY KEY (id),
    CONSTRAINT uq_argus_group_name UNIQUE (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS argus_user_group (
    user_id  VARCHAR(36) NOT NULL,
    group_id VARCHAR(36) NOT NULL,
    CONSTRAINT pk_argus_user_group PRIMARY KEY (user_id, group_id),
    -- 그룹 기준 조회(getUserAndGroups)용. user_id 는 PK 선두라 별도 인덱스가 필요 없다.
    INDEX idx_argus_user_group_group (group_id),
    CONSTRAINT fk_argus_user_group_user
        FOREIGN KEY (user_id) REFERENCES argus_user (id) ON DELETE CASCADE,
    CONSTRAINT fk_argus_user_group_group
        FOREIGN KEY (group_id) REFERENCES argus_group (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS argus_schema_version (
    version    INTEGER  NOT NULL,
    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_argus_schema_version PRIMARY KEY (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT IGNORE INTO argus_schema_version (version) VALUES (1);

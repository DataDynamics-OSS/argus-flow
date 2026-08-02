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
package io.datadynamics.nifi.iaa.db.dao;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * {@code argus_group} 한 행과 그 소속.
 *
 * @param id      불변 식별자(UUID)
 * @param name    그룹 이름
 * @param userIds 소속 사용자의 {@code argus_user.id} 집합
 */
public record GroupRecord(String id, String name, Set<String> userIds) {

    public GroupRecord {
        userIds = userIds == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<>(userIds));
    }
}

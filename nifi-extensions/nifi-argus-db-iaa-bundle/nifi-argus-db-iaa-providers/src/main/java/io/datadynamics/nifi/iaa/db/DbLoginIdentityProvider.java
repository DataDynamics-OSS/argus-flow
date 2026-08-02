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
package io.datadynamics.nifi.iaa.db;

import com.zaxxer.hikari.HikariDataSource;
import io.datadynamics.nifi.iaa.db.dao.UserDao;
import io.datadynamics.nifi.iaa.db.dao.UserRecord;
import io.datadynamics.nifi.iaa.db.password.BcryptPasswordEncoder;
import io.datadynamics.nifi.iaa.db.password.PasswordEncoder;
import io.datadynamics.nifi.iaa.db.schema.SchemaManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authentication.LoginIdentityProviderInitializationContext;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.authentication.exception.ProviderDestructionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RDB 를 사용자 저장소로 쓰는 인증 프로바이더.
 *
 * <p>이 프로바이더는 <strong>비밀번호 검증만</strong> 한다. 사용자가 NiFi 에 존재하는지는
 * authorizers.xml 의 UserGroupProvider 가 판단하므로, DB 에 사용자를 넣어도 인가 쪽이
 * 같은 identity 를 모르면 로그인 직후 권한 없음으로 막힌다. DbUserGroupProvider 와 함께
 * 사용해야 한다.
 *
 * <p>사용자 추가·비밀번호 변경은 매 요청마다 DB 를 조회하므로 즉시 반영된다. 재기동이
 * 필요한 것은 JDBC URL 같은 <em>설정</em>뿐이다.
 */
public class DbLoginIdentityProvider implements LoginIdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(DbLoginIdentityProvider.class);

    public static final String PROP_EXPIRATION = "Authentication Expiration";
    public static final String PROP_AUTO_CREATE_SCHEMA = "Auto Create Schema";
    public static final String PROP_MAX_FAILED = "Max Failed Attempts";
    public static final String PROP_LOCKOUT_DURATION = "Lockout Duration";

    /**
     * 인증 실패 시 항상 이 메시지를 쓴다. 사용자 없음·비밀번호 불일치·비활성·잠금을 구분해
     * 알려주면 계정 열거에 이용된다.
     */
    private static final String GENERIC_FAILURE = "아이디 또는 비밀번호가 올바르지 않습니다.";

    private String identifier;
    private HikariDataSource dataSource;
    private UserDao userDao;
    private PasswordEncoder passwordEncoder;
    private Duration expiration;
    private int maxFailedAttempts;
    private Duration lockoutDuration;

    /**
     * 존재하지 않는 사용자에 대해서도 동일한 비용의 bcrypt 를 수행하기 위한 더미 해시.
     * 기동 시 1회 생성한다 — 소스에 해시를 박아 두지 않기 위함이다.
     */
    private String dummyHash;

    @Override
    public void initialize(final LoginIdentityProviderInitializationContext context) {
        this.identifier = context.getIdentifier();
    }

    @Override
    public void onConfigured(final LoginIdentityProviderConfigurationContext context) {
        final ProviderConfig config = new ProviderConfig(context.getProperties());
        try {
            this.passwordEncoder = new BcryptPasswordEncoder();
            this.dummyHash = passwordEncoder.encode("argus-db-iaa-dummy".toCharArray());

            this.expiration = config.getDuration(PROP_EXPIRATION, Duration.ofHours(12));
            this.maxFailedAttempts = config.getInt(PROP_MAX_FAILED, 5);
            this.lockoutDuration = config.getDuration(PROP_LOCKOUT_DURATION, Duration.ofMinutes(15));

            final Dialect dialect = Dialect.fromJdbcUrl(config.getRequired(DataSourceFactory.PROP_URL));
            this.dataSource = DataSourceFactory.create(config, identifier);
            this.userDao = new UserDao(dataSource);

            new SchemaManager(dataSource, dialect)
                    .ensureSchema(config.getBoolean(PROP_AUTO_CREATE_SCHEMA, false));

            logger.info("DB 인증 프로바이더 [{}] 구성 완료 (방언={}, 만료={}, 최대실패={}, 잠금={})",
                    identifier, dialect, expiration, maxFailedAttempts, lockoutDuration);
        } catch (final SQLException | RuntimeException e) {
            close();
            throw new ProviderCreationException(
                    "DB 인증 프로바이더 [" + identifier + "] 구성에 실패했습니다: " + e.getMessage(), e);
        }
    }

    @Override
    public AuthenticationResponse authenticate(final LoginCredentials credentials)
            throws InvalidLoginCredentialsException, IdentityAccessException {

        final String identity = credentials == null ? null : credentials.getUsername();
        if (identity == null || identity.isBlank()
                || credentials.getPassword() == null || credentials.getPassword().isEmpty()) {
            throw new InvalidLoginCredentialsException(GENERIC_FAILURE);
        }
        final char[] password = credentials.getPassword().toCharArray();

        final Optional<UserRecord> found;
        try {
            found = userDao.findByIdentity(identity);
        } catch (final SQLException e) {
            // DB 장애는 자격증명 오류가 아니다. 구분해야 운영자가 원인을 안다.
            throw new IdentityAccessException("사용자 저장소를 조회할 수 없습니다.", e);
        }

        // 사용자 유무와 무관하게 동일한 비용으로 bcrypt 를 수행한다. 존재하지 않는 계정에
        // 대해 즉시 실패하면 응답 시간 차이로 계정 존재 여부가 드러난다.
        final String hash = found.map(UserRecord::passwordHash).orElse(null);
        final boolean passwordMatches = passwordEncoder.matches(password, hash == null ? dummyHash : hash);

        if (found.isEmpty()) {
            logger.debug("인증 실패 [{}]: 사용자 없음", identity);
            throw new InvalidLoginCredentialsException(GENERIC_FAILURE);
        }
        final UserRecord user = found.get();
        final Instant now = Instant.now();

        if (hash == null) {
            // 비밀번호 인증 대상이 아닌 사용자(인증서·OIDC 전용)
            logger.debug("인증 실패 [{}]: 비밀번호가 설정되지 않은 사용자", identity);
            throw new InvalidLoginCredentialsException(GENERIC_FAILURE);
        }
        if (!user.enabled()) {
            logger.debug("인증 실패 [{}]: 비활성 사용자", identity);
            throw new InvalidLoginCredentialsException(GENERIC_FAILURE);
        }
        if (user.isLockedAt(now)) {
            logger.warn("인증 실패 [{}]: 계정 잠금 상태 (해제 예정 {})", identity, user.lockedUntil());
            throw new InvalidLoginCredentialsException(GENERIC_FAILURE);
        }
        if (!passwordMatches) {
            recordFailure(user, now);
            throw new InvalidLoginCredentialsException(GENERIC_FAILURE);
        }

        try {
            userDao.recordSuccess(user.id());
        } catch (final SQLException e) {
            // 성공 기록 실패로 로그인을 막지는 않는다. 다음 실패 시 카운터가 어긋날 뿐이다.
            logger.warn("로그인 성공 상태를 기록하지 못했습니다 [{}]: {}", identity, e.getMessage());
        }
        logger.debug("인증 성공 [{}]", identity);
        return new AuthenticationResponse(
                user.identity(), user.identity(), expiration.toMillis(), identifier);
    }

    private void recordFailure(final UserRecord user, final Instant now) {
        try {
            final Instant lockUntil = lockoutDuration.isZero() ? null : now.plus(lockoutDuration);
            userDao.recordFailure(user.id(), user.failedCount(), maxFailedAttempts, lockUntil);
            if (user.failedCount() + 1 >= maxFailedAttempts) {
                logger.warn("연속 실패 {}회로 계정을 잠급니다 [{}] (해제 {})",
                        user.failedCount() + 1, user.identity(), lockUntil);
            }
        } catch (final SQLException e) {
            logger.warn("로그인 실패 상태를 기록하지 못했습니다 [{}]: {}", user.identity(), e.getMessage());
        }
    }

    @Override
    public void preDestruction() throws ProviderDestructionException {
        close();
    }

    private void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    /** 테스트에서 풀을 직접 주입하기 위한 진입점. */
    void configureForTesting(final DataSource testDataSource, final Dialect dialect,
                             final int maxFailed, final Duration lockout) throws SQLException {
        this.identifier = "test";
        this.passwordEncoder = new BcryptPasswordEncoder(4);   // 테스트에서는 cost 를 낮춘다
        this.dummyHash = passwordEncoder.encode("dummy".toCharArray());
        this.expiration = Duration.ofHours(12);
        this.maxFailedAttempts = maxFailed;
        this.lockoutDuration = lockout;
        this.userDao = new UserDao(testDataSource);
        new SchemaManager(testDataSource, dialect).ensureSchema(true);
    }

    PasswordEncoder getPasswordEncoder() {
        return passwordEncoder;
    }
}

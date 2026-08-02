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
package io.datadynamics.nifi.processors.csv;

/**
 * 애플리케이션 전역에서 단 하나의 값만 보관하는 간단한 싱글턴 홀더 클래스.
 * init()으로 최초 한 번만 값을 주입할 수 있고, 이후에는 getInstance()로 조회만 가능하다.
 */
public final class Singleton<T> {

	// 전역에서 공유되는 유일한 인스턴스(제네릭 타입이 달라도 하나만 존재)
	private static Singleton<?> instance;

	// 이 싱글턴이 보관하는 실제 값
	private final T value;

	private Singleton(T value) {
		this.value = value;
	}

	/**
	 * 최초 1회만 인스턴스를 주입
	 */
	public static synchronized <T> void init(T value) {
		if (instance != null) {
			throw new IllegalStateException("Already initialized");
		}
		instance = new Singleton<>(value);
	}

	/**
	 * 주입된 인스턴스 반환
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getInstance() {
		if (instance == null) {
			throw new IllegalStateException("Not initialized yet");
		}
		return (T) instance.value;
	}
}
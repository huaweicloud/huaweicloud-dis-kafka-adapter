/*
 * Copyright 2002-2010 the original author or authors.
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
package com.huaweicloud.dis.adapter.kafka.common.utils;

/**
 * Internal class that should be used instead of `Exit.exit()` and `Runtime.getRuntime().halt()` so that tests can
 * easily change the behaviour.
 */
public class Exit {

    public interface Procedure {
        void execute(int statusCode, String message);
    }

    private static final Procedure DEFAULT_HALT_PROCEDURE = new Procedure() {
        @Override
        public void execute(int statusCode, String message) {
            Runtime.getRuntime().halt(statusCode);
        }
    };

    private static final Procedure DEFAULT_EXIT_PROCEDURE = new Procedure() {
        @Override
        public void execute(int statusCode, String message) {
            System.exit(statusCode);
        }
    };

    private volatile static Procedure exitProcedure = DEFAULT_EXIT_PROCEDURE;
    private volatile static Procedure haltProcedure = DEFAULT_HALT_PROCEDURE;

    public static void exit(int statusCode) {
        exit(statusCode, null);
    }

    public static void exit(int statusCode, String message) {
        exitProcedure.execute(statusCode, message);
    }

    public static void halt(int statusCode) {
        halt(statusCode, null);
    }

    public static void halt(int statusCode, String message) {
        haltProcedure.execute(statusCode, message);
    }

    public static void setExitProcedure(Procedure procedure) {
        exitProcedure = procedure;
    }

    public static void setHaltProcedure(Procedure procedure) {
        haltProcedure = procedure;
    }

    public static void resetExitProcedure() {
        exitProcedure = DEFAULT_EXIT_PROCEDURE;
    }

    public static void resetHaltProcedure() {
        haltProcedure = DEFAULT_HALT_PROCEDURE;
    }

}

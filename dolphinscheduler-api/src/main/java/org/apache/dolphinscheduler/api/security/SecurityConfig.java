/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.api.security;

import org.apache.dolphinscheduler.api.security.impl.ldap.LdapAuthenticator;
import org.apache.dolphinscheduler.api.security.impl.oauth2.GoogleOAuth2Authenticator;
import org.apache.dolphinscheduler.api.security.impl.pwd.PasswordAuthenticator;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
// public class SecurityConfig extends WebSecurityConfigurerAdapter {
public class SecurityConfig {

    private static final Logger logger = LoggerFactory.getLogger(SecurityConfig.class);

    @Value("${security.authentication.type:PASSWORD}")
    private String type;

    private AutowireCapableBeanFactory beanFactory;
    private AuthenticationType authenticationType;

    @Autowired
    public SecurityConfig(AutowireCapableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    private void setAuthenticationType(String type) {
        if (StringUtils.isBlank(type)) {
            logger.info("security.authentication.type configuration is empty, the default value 'PASSWORD'");
            this.authenticationType = AuthenticationType.PASSWORD;
            return;
        }

        this.authenticationType = AuthenticationType.valueOf(type);
    }

    @Bean(name = "authenticator")
    public Authenticator authenticator() {
        setAuthenticationType(type);
        Authenticator authenticator;
        switch (authenticationType) {
            case PASSWORD:
                authenticator = new PasswordAuthenticator();
                break;
            case LDAP:
                authenticator = new LdapAuthenticator();
                break;
            case OAUTH2:
                logger.info("[debug111] initializing oauth2 authenticator");
                authenticator = new GoogleOAuth2Authenticator();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + authenticationType);
        }
        beanFactory.autowireBean(authenticator);
        return authenticator;
    }

    // @Override
    // protected void configure(HttpSecurity http) throws Exception {
//        // @formatter:off
//        http
//                .authorizeRequests(a -> a
//                        .antMatchers("/", "/error", "/webjars/**").permitAll()
//                        .anyRequest().authenticated()
//                )
//                .exceptionHandling(e -> e
//                        .authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED))
//                )
//                .csrf(c -> c
//                        .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
//                )
//                .logout(l -> l
//                        .logoutSuccessUrl("/").permitAll()
//                )
//                .oauth2Login();
//        // @formatter:on
    // }

    // @Bean
    // public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
    // logger.info("[debug111] insdie filterChain...");
    // http
    // .authorizeRequests(a -> a
    // .antMatchers("/**", "/ui/**", "/error", "/webjars/**").permitAll()
    // .anyRequest().authenticated())
    // .exceptionHandling(e -> e
    // .authenticationEntryPoint(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED)))
    // .csrf(c -> c
    // .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse()))
    // .logout(l -> l
    // .logoutSuccessUrl("/").permitAll())
    // .oauth2Login();
    // return http.build();
    // }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.authorizeRequests()
                .anyRequest().authenticated()
                .and()
                .oauth2Login();
        return http.build();
    }

    public String getType() {
        return type;
    }
}

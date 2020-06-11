/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.github.adejanovski.cassandra.jdbc;

import org.apache.commons.lang.StringUtils;
import org.apache.http.ssl.PrivateKeyDetails;
import org.apache.http.ssl.PrivateKeyStrategy;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Map;

/**
 * Util class for constructing SSLContext object for SSL
 *
 * @author ssapa
 **/
public class SSLUtil {

    public static SSLContext getSSLContextFromKeyStore(KeyStore clientKeyStore, KeyStore trustStore,
                   String privateKeyAlias, String privateKeyPassphrase) throws SSLParamsException {
        try {
            SSLContext sslContext = SSLContexts.custom()
                    .loadKeyMaterial(clientKeyStore, privateKeyPassphrase.toCharArray(),
                     StringUtils.isEmpty(privateKeyAlias) ? null : new PrivateKeyStrategy() {
                        @Override
                        public String chooseAlias(Map<String, PrivateKeyDetails> aliases,
                                                  Socket socket) {
                            return privateKeyAlias;
                        }
                    })
                    .loadTrustMaterial(trustStore, null)
                    .setKeyStoreType(KeyStore.getDefaultType())
                    .build();
            return sslContext;
        } catch (NoSuchAlgorithmException nsae) {
            throw new SSLParamsException("Invalid Algorithm.", nsae);
        } catch (KeyManagementException kme) {
            throw new SSLParamsException("KeyManagementException: " + kme.getMessage(), kme);
        } catch (UnrecoverableKeyException uke) {
            throw new SSLParamsException("Could not recover keys from client keystore. " +
                    "Check password?", uke);
        } catch (KeyStoreException kse) {
            throw new SSLParamsException("Could not create KeyStore instance [" +
                    kse.getMessage() + "]", kse);
        }
    }

    public static SSLContext getTrustEverybodySSLContext(String keyStoreUrl, String privateKeyAlias
            , String privateKeyPassword, String keyStorePassword, String trustStoreUrl)
            throws SSLParamsException {

        URL ksURL = null;
        try {
            ksURL = new URL(keyStoreUrl);

        } catch (MalformedURLException mue) {
            throw new SSLParamsException(keyStoreUrl + " does not appear to be a valid URL.", mue);
        }
        URL tsURL = null;
        try {
            tsURL = new URL(trustStoreUrl);
        } catch (MalformedURLException mue) {
            throw new SSLParamsException(trustStoreUrl + " does not appear to be a valid URL.", mue);
        }

        try {
            SSLContext sslContext = SSLContexts.custom()
               .loadKeyMaterial(ksURL, keyStorePassword.toCharArray(), privateKeyPassword.toCharArray(),
                  StringUtils.isEmpty(privateKeyAlias) ? null : new PrivateKeyStrategy() {
                                @Override
                      public String chooseAlias(Map<String, PrivateKeyDetails> aliases, Socket socket) {
                         return privateKeyAlias;
                      }
                  })
               .loadTrustMaterial(tsURL, keyStorePassword.toCharArray())
               .setKeyStoreType(KeyStore.getDefaultType())
               .build();
            return sslContext;

        } catch (NoSuchAlgorithmException nsae) {
            throw new SSLParamsException("Invalid Algorithm.", nsae);
        } catch (KeyManagementException kme) {
            throw new SSLParamsException("KeyManagementException: " + kme.getMessage(), kme);
        } catch (CertificateException nsae) {
            throw new SSLParamsException("Could not load client" + KeyStore.getDefaultType() +
                    " keystore from " + keyStoreUrl, nsae);
        } catch (UnrecoverableKeyException uke) {
            throw new SSLParamsException("Could not recover keys from client keystore. " +
                    "Check password?", uke);
        } catch (KeyStoreException kse) {
            throw new SSLParamsException("Could not create KeyStore instance [" +
                    kse.getMessage() + "]", kse);
        } catch (IOException ioe) {
            throw new SSLParamsException("Cannot open " + keyStoreUrl + " [" + ioe.getMessage()
                    + "]", ioe);
        }
    }
}

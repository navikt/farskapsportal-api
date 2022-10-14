package no.nav.farskapsportal.backend.apps.asynkron.config;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import no.nav.farskapsportal.backend.apps.asynkron.config.egenskaper.FarskapsportalAsynkronEgenskaper;
import no.nav.farskapsportal.backend.libs.felles.config.tls.KeyStoreConfig;
import no.nav.security.token.support.client.core.ClientProperties;
import no.nav.security.token.support.client.core.oauth2.OAuth2AccessTokenResponse;
import no.nav.security.token.support.client.core.oauth2.OAuth2AccessTokenService;
import no.nav.security.token.support.client.spring.ClientConfigurationProperties;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RootUriTemplateHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Configuration
@ComponentScan("no.nav.farskapsportal")
public class RestTemplateAsynkronConfig {

  public static final String X_CORRELATION_ID_HEADER_NAME = "X-Correlation-ID";

  private FarskapsportalAsynkronEgenskaper farskapsportalAsynkronEgenskaper;

  public RestTemplateAsynkronConfig(@Autowired FarskapsportalAsynkronEgenskaper farskapsportalAsynkronEgenskaper) {
    this.farskapsportalAsynkronEgenskaper = farskapsportalAsynkronEgenskaper;
  }

  @Bean
  @Scope("prototype")
  @Qualifier("farskapsportal-api")
  public RestTemplate farskapsportalApiRestTemplate(
      @Value("${url.farskapsportal.api.base-url}") String farskapsportalApiRootUrl,
      ClientConfigurationProperties clientConfigurationProperties,
      OAuth2AccessTokenService oAuth2AccessTokenService) {

    var restTemplate = new RestTemplate();

    ClientProperties clientProperties =
        Optional.ofNullable(clientConfigurationProperties.getRegistration().get("farskapsportal-api"))
            .orElseThrow(() -> new RuntimeException("fant ikke oauth2-klientkonfig for farskapsportalApi"));

    restTemplate.getInterceptors().add(accessTokenInterceptor(clientProperties, oAuth2AccessTokenService));
    restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(farskapsportalApiRootUrl));

    return restTemplate;
  }

  @Bean
  @Scope("prototype")
  @Qualifier("skatt")
  public RestTemplate skattRestTemplate(
      @Value("${url.skatt.base-url}") String baseUrl,
      KeyStoreConfig keyStoreConfig)
      throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, KeyManagementException {

    var socketFactory = new SSLConnectionSocketFactory(new SSLContextBuilder()
        .loadTrustMaterial(null, new TrustAllStrategy())
        .loadKeyMaterial(keyStoreConfig.keyStore, keyStoreConfig.keystorePassword.toCharArray()).build(),
        NoopHostnameVerifier.INSTANCE);

    var httpClient = HttpClients.custom().setSSLSocketFactory(socketFactory)
        .setMaxConnTotal(farskapsportalAsynkronEgenskaper.getSkatt().getMaksAntallForbindelser())
        .setMaxConnPerRoute(farskapsportalAsynkronEgenskaper.getSkatt().getMaksAntallForbindelserPerRute())
        .build();

    var requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
    requestFactory.setReadTimeout(farskapsportalAsynkronEgenskaper.getSkatt().getMaksVentetidLesing());
    requestFactory.setConnectTimeout(farskapsportalAsynkronEgenskaper.getSkatt().getMaksVentetidForbindelse());

    var restTemplate = new RestTemplate();
    restTemplate.setRequestFactory(requestFactory);
    restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(baseUrl));

    return restTemplate;
  }

  @Bean
  @Scope("prototype")
  @Qualifier("oppgave")
  public RestTemplate oppgaveRestTemplate(
      @Value("${url.oppgave.base-url}") String oppgaveRootUrl,
      ClientConfigurationProperties clientConfigurationProperties,
      OAuth2AccessTokenService oAuth2AccessTokenService) {

    ClientProperties clientProperties =
        Optional.ofNullable(clientConfigurationProperties.getRegistration().get("oppgave"))
            .orElseThrow(() -> new RuntimeException("fant ikke oauth2-klientkonfig for oppgave"));

    var restTemplate = new RestTemplate();
    restTemplate.getInterceptors().add(accessTokenInterceptor(clientProperties, oAuth2AccessTokenService));

    log.info("Oppretter oppgaveRestTemplate med baseurl {}", oppgaveRootUrl);
    restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(oppgaveRootUrl));

    return restTemplate;
  }

  private ClientHttpRequestInterceptor accessTokenInterceptor(
      ClientProperties clientProperties,
      OAuth2AccessTokenService oAuth2AccessTokenService
  ) {
    return (request, body, execution) -> {
      OAuth2AccessTokenResponse response =
          oAuth2AccessTokenService.getAccessToken(clientProperties);
      request.getHeaders().setBearerAuth(response.getAccessToken());
      return execution.execute(request, body);
    };
  }
}

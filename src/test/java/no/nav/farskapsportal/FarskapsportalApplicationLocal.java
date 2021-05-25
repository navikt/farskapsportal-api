package no.nav.farskapsportal;

import static no.nav.farskapsportal.FarskapsportalApplication.PROFILE_INTEGRATION_TEST;
import static no.nav.farskapsportal.consumer.skatt.SkattEndpointName.MOTTA_FARSKAPSERKLAERING;
import static org.springframework.context.annotation.FilterType.ASSIGNABLE_TYPE;

import com.google.common.net.HttpHeaders;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStore;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import no.digipost.signature.client.Certificates;
import no.digipost.signature.client.ClientConfiguration;
import no.digipost.signature.client.core.Sender;
import no.digipost.signature.client.security.KeyStoreConfig;
import no.nav.bidrag.commons.web.HttpHeaderRestTemplate;
import no.nav.bidrag.commons.web.test.HttpHeaderTestRestTemplate;
import no.nav.farskapsportal.consumer.ConsumerEndpoint;
import no.nav.farskapsportal.consumer.skatt.SkattConsumer;
import no.nav.security.token.support.spring.api.EnableJwtTokenValidation;
import no.nav.security.token.support.test.jersey.TestTokenGeneratorResource;
import no.nav.security.token.support.test.spring.TokenGeneratorConfiguration;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.client.RootUriTemplateHandler;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
@ComponentScan(excludeFilters = {@ComponentScan.Filter(type = ASSIGNABLE_TYPE, value = FarskapsportalApplication.class)})
@EnableJwtTokenValidation(ignore = {"org.springdoc", "org.springframework"})
@Import(TokenGeneratorConfiguration.class)
@Slf4j
public class FarskapsportalApplicationLocal {

  public static final String PROFILE_SCHEDULED_TEST = "scheduled-test";
  public static final String PROFILE_LOCAL_POSTGRES = "local-postgres";
  public static final String PROFILE_REMOTE_POSTGRES = "remote-postgres";
  public static final String PROFILE_LOCAL = "local";
  public static final String PROFILE_TEST = "test";
  public static final String PROFILE_SKATT_SSL_TEST = "skatt-ssl-test";
  private static final String NAV_ORGNR = "123456789";

  public static void main(String... args) {

    String profile = args.length < 1 ? PROFILE_LOCAL : args[0];

    SpringApplication app = new SpringApplication(FarskapsportalApplicationLocal.class);
    app.setAdditionalProfiles(profile);
    app.run(args);
  }


  private static String generateTestToken() {
    TestTokenGeneratorResource testTokenGeneratorResource = new TestTokenGeneratorResource();
    return "Bearer " + testTokenGeneratorResource.issueToken("localhost-idtoken");
  }

  @Bean
  HttpHeaderTestRestTemplate httpHeaderTestRestTemplate() {
    TestRestTemplate testRestTemplate = new TestRestTemplate(new RestTemplateBuilder());
    HttpHeaderTestRestTemplate httpHeaderTestRestTemplate = new HttpHeaderTestRestTemplate(testRestTemplate);
    httpHeaderTestRestTemplate.add(HttpHeaders.AUTHORIZATION, FarskapsportalApplicationLocal::generateTestToken);

    return httpHeaderTestRestTemplate;
  }

  @Bean
  @Profile({PROFILE_TEST, PROFILE_LOCAL, PROFILE_LOCAL_POSTGRES, PROFILE_REMOTE_POSTGRES, PROFILE_SCHEDULED_TEST, PROFILE_SKATT_SSL_TEST})
  public KeyStoreConfig keyStoreConfig() throws IOException {
    var classLoader = getClass().getClassLoader();
    try (InputStream inputStream = classLoader.getResourceAsStream("esigneringkeystore.jceks")) {
      if (inputStream == null) {
        throw new IllegalArgumentException("Fant ikke esigneringkeystore.jceks");
      } else {
        return KeyStoreConfig.fromJavaKeyStore(inputStream, "selfsigned", "changeit", "changeit");
      }
    }
  }

  @Bean
  @Profile({PROFILE_TEST, PROFILE_LOCAL, PROFILE_LOCAL_POSTGRES, PROFILE_REMOTE_POSTGRES, PROFILE_SCHEDULED_TEST, PROFILE_SKATT_SSL_TEST})
  public ClientConfiguration clientConfiguration(KeyStoreConfig keyStoreConfig, @Value("${url.esignering}") String esigneringUrl)
      throws URISyntaxException {
    return ClientConfiguration.builder(keyStoreConfig).trustStore(Certificates.TEST).serviceUri(new URI(esigneringUrl + "/esignering"))
        .globalSender(new Sender(NAV_ORGNR)).build();
  }

  @Lazy
  @Configuration
  @Profile({PROFILE_SKATT_SSL_TEST})
  static class SkattStubSslConfiguration {

    @LocalServerPort
    private int localServerPort;

    @Value("${sertifikat.passord}")
    private String keystorePassword;

    @Value("${sertifikat.keystore-type}")
    private String keystoreType;

    @Value("${sertifikat.keystore-name}")
    private String keystoreName;

    @Bean
    @Qualifier(PROFILE_SKATT_SSL_TEST)
    public HttpHeaderRestTemplate skattLocalIntegrationRestTemplate(@Qualifier("base") HttpHeaderRestTemplate httpHeaderRestTemplate) {

      KeyStore keyStore;
      HttpComponentsClientHttpRequestFactory requestFactory;

      try {
        keyStore = KeyStore.getInstance(keystoreType);
        var classPathResoure = new ClassPathResource(keystoreName);
        var inputStream = classPathResoure.getInputStream();
        keyStore.load(inputStream, keystorePassword.toCharArray());

        var socketFactory = new SSLConnectionSocketFactory(new SSLContextBuilder()
            .loadTrustMaterial(null, new TrustSelfSignedStrategy())
            .loadKeyMaterial(keyStore, keystorePassword.toCharArray()).build(),
            NoopHostnameVerifier.INSTANCE);

        var httpClient = HttpClients.custom().setSSLSocketFactory(socketFactory)
            .setMaxConnTotal(5)
            .setMaxConnPerRoute(5)
            .build();

        requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(10000);
        requestFactory.setConnectTimeout(10000);

        httpHeaderRestTemplate.setRequestFactory(requestFactory);

      } catch (Exception exception) {
        exception.printStackTrace();
      }

      return httpHeaderRestTemplate;
    }

    @Bean
    @Qualifier(PROFILE_INTEGRATION_TEST)
    SkattConsumer skattConsumerIntegrationTest(@Qualifier(PROFILE_SKATT_SSL_TEST) RestTemplate restTemplate,
        @Value("${url.skatt.base-url}") String baseUrl,
        @Value("${url.skatt.registrering-av-farskap}") String endpoint, ConsumerEndpoint consumerEndpoint) {
      log.info("Oppretter SkattConsumer med url {}", baseUrl);
      consumerEndpoint.addEndpoint(MOTTA_FARSKAPSERKLAERING, endpoint);
      restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(baseUrl));
      return new SkattConsumer(restTemplate, consumerEndpoint);
    }

    @Bean
    @Qualifier("sikret")
    SkattConsumer skattConsumerSikret(@Qualifier(PROFILE_SKATT_SSL_TEST) RestTemplate restTemplate,
        @Value("${url.skatt.registrering-av-farskap}") String endpoint, ConsumerEndpoint consumerEndpoint) {

      var baseUrl = "https://localhost:" + localServerPort;

      consumerEndpoint.addEndpoint(MOTTA_FARSKAPSERKLAERING, endpoint);
      restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(baseUrl));
      return new SkattConsumer(restTemplate, consumerEndpoint);
    }

    @Bean
    @Qualifier("usikret")
    SkattConsumer skattConsumerUsikret(@Qualifier("base") RestTemplate restTemplate,
        @Value("${url.skatt.registrering-av-farskap}") String endpoint, ConsumerEndpoint consumerEndpoint) {

      var baseUrl = "http://localhost:" + localServerPort;

      consumerEndpoint.addEndpoint(MOTTA_FARSKAPSERKLAERING, endpoint);
      restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(baseUrl));
      return new SkattConsumer(restTemplate, consumerEndpoint);
    }
  }

  @Configuration
  @Profile({PROFILE_LOCAL_POSTGRES, PROFILE_REMOTE_POSTGRES})
  static class FlywayConfiguration {

    @Autowired
    public FlywayConfiguration(@Qualifier("dataSource") DataSource dataSource) {
      Flyway.configure().ignoreMissingMigrations(true).baselineOnMigrate(true).dataSource(dataSource).load().migrate();
    }
  }

  @Lazy
  @Configuration
  @Profile(PROFILE_SCHEDULED_TEST)
  static class SkattStubConfiguration {

    @LocalServerPort
    private int localServerPort;

    @Bean
    SkattConsumer skattConsumer(@Qualifier("skatt") RestTemplate restTemplate,
        @Value("${url.skatt.registrering-av-farskap}") String endpoint, ConsumerEndpoint consumerEndpoint) {

      var baseUrl = "http://localhost:" + localServerPort;
      log.info("baseUrl: {}", baseUrl);

      consumerEndpoint.addEndpoint(MOTTA_FARSKAPSERKLAERING, endpoint);
      restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(baseUrl));
      return new SkattConsumer(restTemplate, consumerEndpoint);
    }
  }
}

package no.nav.farskapsportal.backend.apps.asynkron.config;

import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_INTEGRATION_TEST;
import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_LIVE;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.api.FarskapsportalApiConsumer;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.api.FarskapsportalApiEndpoint;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.oppgave.OppgaveApiConsumer;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.oppgave.OppgaveApiConsumerEndpoint;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.skatt.SkattConsumer;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.skatt.SkattEndpoint;
import no.nav.farskapsportal.backend.libs.felles.config.tls.KeyStoreConfig;
import no.nav.farskapsportal.backend.libs.felles.consumer.ConsumerEndpoint;
import no.nav.farskapsportal.backend.libs.felles.secretmanager.AccessSecretVersion;
import no.nav.farskapsportal.backend.libs.felles.secretmanager.FarskapKeystoreCredentials;
import no.nav.security.token.support.client.spring.oauth2.EnableOAuth2Client;
import no.nav.security.token.support.core.context.TokenValidationContextHolder;
import no.nav.security.token.support.spring.SpringTokenValidationContextHolder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Configuration
@EnableOAuth2Client
@ComponentScan("no.nav.farskapsportal.backend")
public class FarskapsportalAsynkronConfig {

  public static final String PROFILE_SCHEDULED_TEST = "scheduled-test";

  @Bean
  public TokenValidationContextHolder oidcRequestContextHolder() {
    return new SpringTokenValidationContextHolder();
  }


  @Bean
  @Profile({PROFILE_LIVE, PROFILE_INTEGRATION_TEST})
  public KeyStoreConfig keyStoreConfig(
      @Value("${virksomhetssertifikat.prosjektid}") String virksomhetssertifikatProsjektid,
      @Value("${virksomhetssertifikat.hemmelighetnavn}") String virksomhetssertifikatHemmelighetNavn,
      @Value("${virksomhetssertifikat.hemmelighetversjon}") String virksomhetssertifikatHemmelighetVersjon,
      @Value("${virksomhetssertifikat.passord.prosjektid}") String virksomhetssertifikatPassordProsjektid,
      @Value("${virksomhetssertifikat.passord.hemmelighetnavn}") String virksomhetssertifikatPassordHemmelighetNavn,
      @Value("${virksomhetssertifikat.passord.hemmelighetversjon}") String virksomhetssertifikatPassordHemmelighetVersjon,
      @Autowired(required = false) AccessSecretVersion accessSecretVersion) throws IOException {

    var sertifikatpassord = accessSecretVersion
        .accessSecretVersion(virksomhetssertifikatPassordProsjektid, virksomhetssertifikatPassordHemmelighetNavn,
            virksomhetssertifikatPassordHemmelighetVersjon).getData().toStringUtf8();

    var objectMapper = new ObjectMapper();
    var farskapKeystoreCredentials = objectMapper.readValue(sertifikatpassord, FarskapKeystoreCredentials.class);

    log.info("lengde sertifikatpassord {}", farskapKeystoreCredentials.getPassword().length());

    var secretPayload = accessSecretVersion
        .accessSecretVersion(virksomhetssertifikatProsjektid, virksomhetssertifikatHemmelighetNavn, virksomhetssertifikatHemmelighetVersjon);

    log.info("lengde sertifikat: {}", secretPayload.getData().size());
    var inputStream = new ByteArrayInputStream(secretPayload.getData().toByteArray());

    return KeyStoreConfig
        .fromJavaKeyStore(inputStream, farskapKeystoreCredentials.getAlias(), farskapKeystoreCredentials.getPassword(),
            farskapKeystoreCredentials.getPassword());
  }

  @Bean
  public FarskapsportalApiConsumer farskapsportalApiConsumer(
      @Qualifier("farskapsportal-api") RestTemplate restTemplate,
      @Value("${url.farskapsportal.api.synkronisere-signeringsstatus}") String synkronisereSigneringsstatusEndpoint,
      @Value("${url.farskapsportal.api.hente-aktoerid}") String henteAktoeridEndpoint,
      ConsumerEndpoint consumerEndpoint) {

    consumerEndpoint.addEndpoint(FarskapsportalApiEndpoint.SYNKRONISERE_SIGNERINGSSTATUS_ENDPOINT_NAME, synkronisereSigneringsstatusEndpoint);
    consumerEndpoint.addEndpoint(FarskapsportalApiEndpoint.HENTE_AKTOERID_ENDPOINT_NAME, henteAktoeridEndpoint);

    return new FarskapsportalApiConsumer(restTemplate, consumerEndpoint);
  }

  @Bean
  public OppgaveApiConsumer oppgaveApiConsumer(
      @Qualifier("oppgave") RestTemplate restTemplate,
      @Value("${url.oppgave.opprette}") String oppretteOppgaveEndpoint,
      ConsumerEndpoint consumerEndpoint) {
    consumerEndpoint.addEndpoint(OppgaveApiConsumerEndpoint.OPPRETTE_OPPGAVE_ENDPOINT_NAME, oppretteOppgaveEndpoint);
    return new OppgaveApiConsumer(restTemplate, consumerEndpoint);
  }

  @Bean
  SkattConsumer skattConsumer(PoolingHttpClientConnectionManager httpClientConnectionManager,
      @Value("url.skatt.base-url") String skattBaseUrl,
      @Value("${url.skatt.registrering-av-farskap}") String endpoint,
      ConsumerEndpoint consumerEndpoint) {
    consumerEndpoint.addEndpoint(SkattEndpoint.MOTTA_FARSKAPSERKLAERING, skattBaseUrl + endpoint);
    return new SkattConsumer(httpClientConnectionManager, consumerEndpoint);
  }
}

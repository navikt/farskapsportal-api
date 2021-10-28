package no.nav.farskapsportal.backend.apps.asynkron.config;

import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_INTEGRATION_TEST;
import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_LIVE;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.joark.FarskapsportalJoarkMapper;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.joark.JournalpostApiConsumer;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.joark.JournalpostApiConsumerEndpointName;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.skatt.SkattConsumer;
import no.nav.farskapsportal.backend.apps.asynkron.consumer.skatt.SkattEndpointName;
import no.nav.farskapsportal.backend.libs.felles.secretmanager.AccessSecretVersion;
import no.nav.farskapsportal.backend.libs.felles.secretmanager.FarskapKeystoreCredentials;
import no.nav.farskapsportal.backend.libs.felles.config.tls.KeyStoreConfig;
import no.nav.farskapsportal.backend.libs.felles.consumer.ConsumerEndpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RootUriTemplateHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Configuration
@ComponentScan("no.nav.farskapsportal.backend")
public class FarskapsportalAsynkronConfig {

  public static final String PROFILE_SCHEDULED_TEST = "scheduled-test";

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
  public JournalpostApiConsumer journalpostApiConsumer(
      @Qualifier("journalpostapi") RestTemplate restTemplate,
      @Value("${url.joark.base-url}") String journalpostapiUrl,
      @Value("${url.joark.opprette-journalpost}") String journalpostapiEndpoint,
      ConsumerEndpoint consumerEndpoint,
      FarskapsportalJoarkMapper farskapsportalJoarkMapper) {
    consumerEndpoint.addEndpoint(JournalpostApiConsumerEndpointName.ARKIVERE_JOURNALPOST, journalpostapiEndpoint);
    restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(journalpostapiUrl));
    log.info("Oppretter JournalpostApiConsumer med url {}", journalpostapiUrl);
    return new JournalpostApiConsumer(restTemplate, consumerEndpoint, farskapsportalJoarkMapper);
  }

  @Bean
  SkattConsumer skattConsumer(@Qualifier("skatt") RestTemplate restTemplate,
      @Value("${url.skatt.base-url}") String baseUrl,
      @Value("${url.skatt.registrering-av-farskap}") String endpoint,
      ConsumerEndpoint consumerEndpoint) {
    log.info("Oppretter SkattConsumer med url {}", baseUrl);
    consumerEndpoint.addEndpoint(SkattEndpointName.MOTTA_FARSKAPSERKLAERING, endpoint);
    restTemplate.setUriTemplateHandler(new RootUriTemplateHandler(baseUrl));
    return new SkattConsumer(restTemplate, consumerEndpoint);
  }
}

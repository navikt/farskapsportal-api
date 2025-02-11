package no.nav.farskapsportal.backend.libs.felles.config.egenskaper;

import lombok.Getter;
import lombok.Setter;
import no.nav.farskapsportal.backend.libs.felles.config.egenskaper.yaml.YamlPropertySourceFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "brukernotifikasjon")
@PropertySource(
    value = "classpath:felles-application.yml",
    factory = YamlPropertySourceFactory.class)
public class Brukernotifikasjon {

  private String topicBeskjed;
  private String topicFerdig;
  private String topicOppgave;
  private String grupperingsidFarskap;
  private int synlighetBeskjedAntallMaaneder;
  private int levetidOppgaveAntallDager;
  private int sikkerhetsnivaaBeskjed;
  private boolean skruddPaa;
}

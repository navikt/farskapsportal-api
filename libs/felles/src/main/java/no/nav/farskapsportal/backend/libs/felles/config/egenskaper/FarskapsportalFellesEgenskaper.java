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
@ConfigurationProperties(prefix = "farskapsportal.felles.egenskaper")
@PropertySource(
    value = "classpath:felles-application.yml",
    factory = YamlPropertySourceFactory.class)
public class FarskapsportalFellesEgenskaper {

  private String appnavn;
  private String naisClusternavn;
  private int maksAntallMaanederEtterFoedsel;
  private int kontrollFarMaksAntallForsoek;
  private String url;
  private Brukernotifikasjon brukernotifikasjon;
}

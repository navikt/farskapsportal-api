package no.nav.farskapsportal.backend.libs.felles.consumer.brukernotifikasjon;

import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_TEST;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import java.util.UUID;
import no.nav.brukernotifikasjon.schemas.Beskjed;
import no.nav.brukernotifikasjon.schemas.Nokkel;
import no.nav.brukernotifikasjon.schemas.builders.NokkelBuilder;
import no.nav.farskapsportal.backend.libs.entity.Forelder;
import no.nav.farskapsportal.backend.libs.felles.FarskapsportalFellesTestConfig;
import no.nav.farskapsportal.backend.libs.felles.config.egenskaper.FarskapsportalFellesEgenskaper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@DisplayName("Beskjedprodusent")
@SpringBootTest(classes = FarskapsportalFellesTestConfig.class)
@ActiveProfiles(PROFILE_TEST)
public class BeskjedprodusentTest {

  @Autowired
  private FarskapsportalFellesEgenskaper farskapsportalFellesEgenskaper;

  @MockBean
  private KafkaTemplate<Nokkel, Beskjed> ferdigkoe;

  @Autowired
  private Beskjedprodusent beskjedprodusent;

  @Test
  void skalOppretteBeskjedTilBruker() throws MalformedURLException {

    // given
    var noekkelfanger = ArgumentCaptor.forClass(Nokkel.class);
    var beskjedfanger = ArgumentCaptor.forClass(Beskjed.class);
    var eksternVarsling = true;

    var far = Forelder.builder().foedselsnummer("11111122222").build();
    var farskapsportalUrl = new URL(farskapsportalFellesEgenskaper.getUrl());

    // when
    beskjedprodusent.oppretteBeskjedTilBruker(far, "Hei på deg", eksternVarsling, oppretteNokkel());

    //then
    verify(ferdigkoe, times(1))
        .send(eq(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicBeskjed()), noekkelfanger.capture(), beskjedfanger.capture());

    var noekler = noekkelfanger.getAllValues();
    var beskjeder = beskjedfanger.getAllValues();

    assertAll(
        () -> assertThat(noekler.size()).isEqualTo(1),
        () -> assertThat(beskjeder.size()).isEqualTo(1));

    var noekkel = noekler.get(0);
    var beskjed = beskjeder.get(0);

    assertAll(
        () -> assertThat(beskjed.getFodselsnummer()).isEqualTo(far.getFoedselsnummer()),
        () -> assertThat(beskjed.getGrupperingsId()).isEqualTo(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getGrupperingsidFarskap()),
        () -> assertThat(LocalDateTime.ofInstant(Instant.ofEpochMilli(beskjed.getTidspunkt()),
            ZoneId.of("UTC"))).isBetween(ZonedDateTime.now(ZoneId.of("UTC")).toLocalDateTime().minusSeconds(2),
            ZonedDateTime.now(ZoneId.of("UTC")).toLocalDateTime()),
        () -> assertThat(beskjed.getEksternVarsling()).isEqualTo(eksternVarsling),
        () -> assertThat(beskjed.getLink()).isEqualTo(farskapsportalUrl.toString()),
        () -> assertThat(beskjed.getSikkerhetsnivaa()).isEqualTo(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getSikkerhetsnivaaBeskjed()),
        () -> assertThat(beskjed.getTekst()).isEqualTo("Hei på deg"),
        () -> assertThat(LocalDate.ofInstant(Instant.ofEpochMilli(beskjed.getSynligFremTil()),
            ZoneId.of("UTC"))).isEqualTo(LocalDate.ofInstant(Instant.ofEpochMilli(beskjed.getTidspunkt()),
            TimeZone.getDefault().toZoneId()).plusMonths(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getSynlighetBeskjedAntallMaaneder())),
        () -> assertThat(noekkel.getSystembruker()).isEqualTo(farskapsportalFellesEgenskaper.getSystembrukerBrukernavn())
    );
  }

  private Nokkel oppretteNokkel() {
    var unikEventid = UUID.randomUUID().toString();
    return new NokkelBuilder().withSystembruker(farskapsportalFellesEgenskaper.getSystembrukerBrukernavn()).withEventId(unikEventid).build();
  }
}

package no.nav.farskapsportal.backend.libs.felles.consumer.brukernotifikasjon;

import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.henteBarnUtenFnr;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.henteForelder;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.lageUrl;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;
import no.nav.brukernotifikasjon.schemas.input.NokkelInput;
import no.nav.brukernotifikasjon.schemas.input.OppgaveInput;
import no.nav.farskapsportal.backend.libs.dto.Forelderrolle;
import no.nav.farskapsportal.backend.libs.entity.Barn;
import no.nav.farskapsportal.backend.libs.entity.Dokument;
import no.nav.farskapsportal.backend.libs.entity.Farskapserklaering;
import no.nav.farskapsportal.backend.libs.entity.Forelder;
import no.nav.farskapsportal.backend.libs.entity.Signeringsinformasjon;
import no.nav.farskapsportal.backend.libs.felles.FarskapsportalFellesTestConfig;
import no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig;
import no.nav.farskapsportal.backend.libs.felles.config.egenskaper.FarskapsportalFellesEgenskaper;
import no.nav.farskapsportal.backend.libs.felles.consumer.bucket.GcpStorageManager;
import no.nav.farskapsportal.backend.libs.felles.persistence.dao.FarskapserklaeringDao;
import no.nav.farskapsportal.backend.libs.felles.persistence.dao.OppgavebestillingDao;
import no.nav.farskapsportal.backend.libs.felles.service.PersistenceService;
import no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils;
import no.nav.tms.varsel.action.Sensitivitet;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = FarskapsportalFellesTestConfig.class)
@ActiveProfiles(FarskapsportalFellesConfig.PROFILE_TEST)
@AutoConfigureWireMock(port = 0)
public class VarselprodusentTest {

  @Value("${wiremock.server.port}")
  private String wiremockPort;

  private @Autowired FarskapsportalFellesEgenskaper farskapsportalFellesEgenskaper;
  private @Autowired Varselprodusent varselprodusent;
  private @Autowired PersistenceService persistenceService;
  private @Autowired FarskapserklaeringDao farskapserklaeringDao;
  private @Autowired OppgavebestillingDao oppgavebestillingDao;
  private @MockBean KafkaTemplate<String, String> varselkø;
  private @MockBean GcpStorageManager gcpStorageManager;

  @Test
  void skalOppretteOppgaveForSigneringAvFarskapserklaering() {

    // given
    oppgavebestillingDao.deleteAll();
    farskapserklaeringDao.deleteAll();

    var noekkelfanger = ArgumentCaptor.forClass(String.class);
    var oppgavefanger = ArgumentCaptor.forClass(String.class);

    var far = Forelder.builder().foedselsnummer("11111122222").build();
    var oppgavetekst = "Vennligst signer farskapserklæringen";
    var eksternVarsling = false;

    var farskapserklaeringSomVenterPaaFarsSignatur =
        henteFarskapserklaering(
            henteForelder(Forelderrolle.MOR),
            henteForelder(Forelderrolle.FAR),
            TestUtils.henteBarnUtenFnr(5));
    farskapserklaeringSomVenterPaaFarsSignatur
        .getDokument()
        .getSigneringsinformasjonMor()
        .setSigneringstidspunkt(LocalDateTime.now().minusMinutes(3));
    var lagretFarskapserklaering =
        persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

    // when
    varselprodusent.oppretteOppgaveForSigneringAvFarskapserklaering(
        lagretFarskapserklaering.getId(), far);

    // then
    verify(varselkø, times(1))
        .send(
            eq(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicOppgave()),
            noekkelfanger.capture(),
            oppgavefanger.capture());

    var noekler = noekkelfanger.getAllValues();
    var oppgaver = oppgavefanger.getAllValues();

    var oppgavebestilling =
        persistenceService
            .henteAktiveOppgaverTilForelderIFarskapserklaering(
                lagretFarskapserklaering.getId(), lagretFarskapserklaering.getFar())
            .stream()
            .findFirst();

    assertAll(
        () -> assertThat(oppgavebestilling).isPresent(),
        () -> assertThat(oppgavebestilling.get().getFerdigstilt()).isNull(),
        () -> assertThat(oppgavebestilling.get().getOpprettet()).isNotNull(),
        () ->
            assertThat(oppgavebestilling.get().getOpprettet())
                .isBefore(LocalDateTime.now().plusMinutes(10)),
        () ->
            assertThat(oppgavebestilling.get().getOpprettet())
                .isAfter(LocalDateTime.now().minusMinutes(10)),
        () -> assertThat(oppgavebestilling.get().getEventId()).isNotNull(),
        () -> assertThat(oppgavebestilling.get().getForelder()).isNotNull(),
        () -> assertThat(noekler.size()).isEqualTo(1),
        () -> assertThat(oppgaver.size()).isEqualTo(1));

    var noekkel = noekler.get(0);
    var oppgave = oppgaver.get(0);

    assertAll(
        () -> assertThat(noekkel.getFodselsnummer()).isEqualTo(far.getFoedselsnummer()),
        () -> assertThat(oppgave.getEksternVarsling()).isEqualTo(eksternVarsling),
        () -> assertThat(oppgave.getTekst()).isEqualTo(oppgavetekst),
        () ->
            assertThat(noekkel.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () -> assertThat(oppgave.getSikkerhetsnivaa()).isEqualTo(Sensitivitet.High),
        () ->
            assertThat(oppgave.getTidspunkt())
                .isBetween(
                    Instant.now().minusSeconds(5).toEpochMilli(), Instant.now().toEpochMilli()),
        () -> assertThat(noekkel.getEventId()).isEqualTo(oppgavebestilling.get().getEventId()));
  }

  @Test
  void skalIkkeOppretteDuplikatOppgavebestilling() {

    // given
    oppgavebestillingDao.deleteAll();
    farskapserklaeringDao.deleteAll();

    var farskapserklaeringSomVenterPaaFarsSignatur =
        henteFarskapserklaering(
            henteForelder(Forelderrolle.MOR),
            henteForelder(Forelderrolle.FAR),
            henteBarnUtenFnr(5));
    farskapserklaeringSomVenterPaaFarsSignatur
        .getDokument()
        .getSigneringsinformasjonMor()
        .setSigneringstidspunkt(LocalDateTime.now().minusMinutes(3));
    var lagretFarskapserklaering =
        persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

    persistenceService.lagreNyOppgavebestilling(
        lagretFarskapserklaering.getId(), UUID.randomUUID().toString());

    // when
    varselprodusent.oppretteOppgaveForSigneringAvFarskapserklaering(
        lagretFarskapserklaering.getId(), lagretFarskapserklaering.getFar());

    // then
    verify(varselkø, times(0)).send(anyString(), any(NokkelInput.class), any(OppgaveInput.class));
  }

  private Farskapserklaering henteFarskapserklaering(Forelder mor, Forelder far, Barn barn) {

    var dokument =
        Dokument.builder()
            .navn("farskapserklaering.pdf")
            .signeringsinformasjonMor(
                Signeringsinformasjon.builder()
                    .redirectUrl(lageUrl(wiremockPort, "redirect-mor"))
                    .signeringstidspunkt(LocalDateTime.now())
                    .build())
            .signeringsinformasjonFar(
                Signeringsinformasjon.builder()
                    .redirectUrl(lageUrl(wiremockPort, "/redirect-far"))
                    .build())
            .build();

    return Farskapserklaering.builder().barn(barn).mor(mor).far(far).dokument(dokument).build();
  }
}

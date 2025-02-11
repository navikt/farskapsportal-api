package no.nav.farskapsportal.backend.libs.felles.consumer.brukernotifikasjon;

import static no.nav.farskapsportal.backend.libs.felles.config.FarskapsportalFellesConfig.PROFILE_TEST;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.FAR;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.MOR;
import static no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils.henteBarnMedFnr;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import no.nav.brukernotifikasjon.schemas.input.BeskjedInput;
import no.nav.brukernotifikasjon.schemas.input.DoneInput;
import no.nav.brukernotifikasjon.schemas.input.NokkelInput;
import no.nav.brukernotifikasjon.schemas.input.OppgaveInput;
import no.nav.farskapsportal.backend.libs.dto.Forelderrolle;
import no.nav.farskapsportal.backend.libs.entity.Barn;
import no.nav.farskapsportal.backend.libs.entity.Dokument;
import no.nav.farskapsportal.backend.libs.entity.Farskapserklaering;
import no.nav.farskapsportal.backend.libs.entity.Oppgavebestilling;
import no.nav.farskapsportal.backend.libs.entity.Signeringsinformasjon;
import no.nav.farskapsportal.backend.libs.felles.FarskapsportalFellesTestConfig;
import no.nav.farskapsportal.backend.libs.felles.config.BrukernotifikasjonConfig;
import no.nav.farskapsportal.backend.libs.felles.config.egenskaper.FarskapsportalFellesEgenskaper;
import no.nav.farskapsportal.backend.libs.felles.consumer.bucket.GcpStorageManager;
import no.nav.farskapsportal.backend.libs.felles.persistence.dao.FarskapserklaeringDao;
import no.nav.farskapsportal.backend.libs.felles.persistence.dao.OppgavebestillingDao;
import no.nav.farskapsportal.backend.libs.felles.service.PersistenceService;
import no.nav.farskapsportal.backend.libs.felles.test.utils.TestUtils;
import no.nav.tms.varsel.action.Sensitivitet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@DisplayName("Teste BrukernotifikasjonConsumer")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = {FarskapsportalFellesTestConfig.class, BrukernotifikasjonConfig.class})
@ActiveProfiles(PROFILE_TEST)
@AutoConfigureWireMock(port = 0)
public class BrukernotifikasjonConsumerTest {

  private static final String MELDING_OM_VENTENDE_FARSKAPSERKLAERING =
      "Du har mottatt en farskapserklæring som venter på din signatur.";
  private static final String MELDING_TIL_MOR_OM_AVBRUTT_SIGNERING =
      "Fars signering ble avbrutt, aktuell farskapserklæring måtte derfor slettes. Mor kan opprette ny hvis ønskelig. Trykk her for å opprette ny farskapserklæring.";
  private static final String MELDING_TIL_FAR_OM_AVBRUTT_SIGNERING =
      "Fars signering ble avbrutt, aktuell farskapserklæring måtte derfor slettes. Mor kan opprette ny hvis ønskelig.";
  private static final String MELDING_OM_MANGLENDE_SIGNERING =
      "Aksjon kreves: Farskapserklæring opprettet den %s for barn med %s er ikke ferdigstilt. Våre systemer mangler informasjon om at far har signert. Far må logge inn på Farskapsportal og forsøke å signere eller oppdatere status på ny. Ta kontakt med NAV ved problemer.";
  private static final String MELDING_OM_IKKE_UTFOERT_SIGNERINGSOPPGAVE =
      "Far har ikke signert farskapserklæringen innen fristen. Farskapserklæringen er derfor slettet. Mor kan opprette ny hvis ønskelig. Trykk her for å opprette ny farskapserklæring.";
  private static final Barn BARN = TestUtils.henteBarnUtenFnr(5);

  private @Value("${wiremock.server.port}") String wiremockPort;
  private @Autowired BrukernotifikasjonConsumer brukernotifikasjonConsumer;
  private @Autowired PersistenceService persistenceService;
  private @Autowired FarskapserklaeringDao farskapserklaeringDao;
  private @Autowired OppgavebestillingDao oppgavebestillingDao;
  private @MockBean KafkaTemplate<NokkelInput, BeskjedInput> beskjedkoe;
  private @MockBean KafkaTemplate<NokkelInput, DoneInput> ferdigkoe;
  private @MockBean KafkaTemplate<NokkelInput, OppgaveInput> oppgavekoe;
  private @MockBean GcpStorageManager gcpStorageManager;
  private @Autowired FarskapsportalFellesEgenskaper farskapsportalFellesEgenskaper;

  @Test
  void skalInformereForeldreOmTilgjengeligFarskapserklaering() {

    // given
    var noekkelfanger = ArgumentCaptor.forClass(NokkelInput.class);
    var beskjedfanger = ArgumentCaptor.forClass(BeskjedInput.class);

    // when
    brukernotifikasjonConsumer.informereForeldreOmTilgjengeligFarskapserklaering(MOR, FAR);

    // then
    verify(beskjedkoe, times(2))
        .send(
            eq(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicBeskjed()),
            noekkelfanger.capture(),
            beskjedfanger.capture());

    var noekler = noekkelfanger.getAllValues();
    var beskjeder = beskjedfanger.getAllValues();

    assertAll(
        () -> assertThat(noekler.size()).isEqualTo(2),
        () -> assertThat(beskjeder.size()).isEqualTo(2));

    var noekkelMor = noekler.get(0);
    var noekkelFar = noekler.get(1);
    var beskjedMor = beskjeder.get(0);
    var beskjedFar = beskjeder.get(1);

    var beskjedMorSynligFremTilDato =
        Instant.ofEpochMilli(beskjedMor.getSynligFremTil())
            .atZone(ZoneId.systemDefault())
            .toLocalDate();
    var beskjedFarSynligFremTilDato =
        Instant.ofEpochMilli(beskjedFar.getSynligFremTil())
            .atZone(ZoneId.systemDefault())
            .toLocalDate();

    assertAll(
        () -> assertThat(beskjedMor.getEksternVarsling()).isTrue(),
        () -> assertThat(beskjedFar.getEksternVarsling()).isTrue(),
        () ->
            assertThat(noekkelMor.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(noekkelFar.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () -> assertThat(noekkelMor.getFodselsnummer()).isEqualTo(MOR.getFoedselsnummer()),
        () -> assertThat(noekkelFar.getFodselsnummer()).isEqualTo(FAR.getFoedselsnummer()),
        () ->
            assertThat(beskjedMor.getSikkerhetsnivaa())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getSikkerhetsnivaaBeskjed()),
        () ->
            assertThat(beskjedFar.getSikkerhetsnivaa())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getSikkerhetsnivaaBeskjed()),
        () ->
            assertThat(noekkelMor.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(noekkelFar.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(beskjedMor.getLink())
                .isEqualTo(farskapsportalFellesEgenskaper.getUrl() + "/oversikt"),
        () ->
            assertThat(beskjedFar.getLink())
                .isEqualTo(farskapsportalFellesEgenskaper.getUrl() + "/oversikt"),
        () ->
            assertThat(beskjedMorSynligFremTilDato)
                .isEqualTo(
                    LocalDate.now()
                        .plusMonths(
                            farskapsportalFellesEgenskaper
                                .getBrukernotifikasjon()
                                .getSynlighetBeskjedAntallMaaneder())),
        () ->
            assertThat(beskjedFarSynligFremTilDato)
                .isEqualTo(
                    LocalDate.now()
                        .plusMonths(
                            farskapsportalFellesEgenskaper
                                .getBrukernotifikasjon()
                                .getSynlighetBeskjedAntallMaaneder())));
  }

  @Test
  void skalVarsleMorOmUtloeptOppgaveForSignering() {

    // given
    oppgavebestillingDao.deleteAll();
    farskapserklaeringDao.deleteAll();

    var noekkelfanger = ArgumentCaptor.forClass(NokkelInput.class);
    var beskjedfanger = ArgumentCaptor.forClass(BeskjedInput.class);

    // when
    brukernotifikasjonConsumer.varsleMorOmUtgaattOppgaveForSignering(MOR);

    // then
    verify(beskjedkoe, times(1))
        .send(
            eq(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicBeskjed()),
            noekkelfanger.capture(),
            beskjedfanger.capture());

    var nokkel = noekkelfanger.getValue();
    var beskjed = beskjedfanger.getValue();

    assertAll(
        () -> assertThat(beskjed.getEksternVarsling()).isTrue(),
        () -> assertThat(nokkel.getFodselsnummer()).isEqualTo(MOR.getFoedselsnummer()),
        () ->
            assertThat(nokkel.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(beskjed.getSikkerhetsnivaa())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getSikkerhetsnivaaBeskjed()),
        () ->
            assertThat(beskjed.getTidspunkt())
                .isBetween(
                    Instant.now().minusSeconds(5).toEpochMilli(), Instant.now().toEpochMilli()),
        () -> assertThat(beskjed.getTekst()).isEqualTo(MELDING_OM_IKKE_UTFOERT_SIGNERINGSOPPGAVE),
        () -> assertThat(beskjed.getLink()).isEqualTo(farskapsportalFellesEgenskaper.getUrl()));
  }

  @Test
  void skalVarsleMorOgFarDersomFarAvbryterSignering() {

    // given
    var noekkelfanger = ArgumentCaptor.forClass(NokkelInput.class);
    var beskjedfanger = ArgumentCaptor.forClass(BeskjedInput.class);

    // when
    brukernotifikasjonConsumer.varsleOmAvbruttSignering(MOR, FAR);

    // then
    verify(beskjedkoe, times(2))
        .send(
            eq(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicBeskjed()),
            noekkelfanger.capture(),
            beskjedfanger.capture());

    var alleBeskjeder = beskjedfanger.getAllValues();
    var beskjedTilMor = alleBeskjeder.get(0);
    var beskjedTilFar = alleBeskjeder.get(1);

    var alleNoekler = noekkelfanger.getAllValues();
    var noekkelTilMor = alleNoekler.get(0);
    var noekkelTilFar = alleNoekler.get(1);

    assertAll(
        () -> assertThat(beskjedTilMor.getEksternVarsling()).isTrue(),
        () -> assertThat(noekkelTilMor.getFodselsnummer()).isEqualTo(MOR.getFoedselsnummer()),
        () ->
            assertThat(noekkelTilMor.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(beskjedTilMor.getSikkerhetsnivaa())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getSikkerhetsnivaaBeskjed()),
        () ->
            assertThat(beskjedTilMor.getTidspunkt())
                .isBetween(
                    Instant.now().minusSeconds(5).toEpochMilli(), Instant.now().toEpochMilli()),
        () -> assertThat(beskjedTilMor.getTekst()).isEqualTo(MELDING_TIL_MOR_OM_AVBRUTT_SIGNERING),
        () ->
            assertThat(beskjedTilMor.getLink()).isEqualTo(farskapsportalFellesEgenskaper.getUrl()));

    assertAll(
        () -> assertThat(beskjedTilFar.getEksternVarsling()).isTrue(),
        () -> assertThat(noekkelTilFar.getFodselsnummer()).isEqualTo(FAR.getFoedselsnummer()),
        () ->
            assertThat(noekkelTilFar.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(beskjedTilFar.getSikkerhetsnivaa())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getSikkerhetsnivaaBeskjed()),
        () ->
            assertThat(beskjedTilFar.getTidspunkt())
                .isBetween(
                    Instant.now().minusSeconds(5).toEpochMilli(), Instant.now().toEpochMilli()),
        () -> assertThat(beskjedTilFar.getTekst()).isEqualTo(MELDING_TIL_FAR_OM_AVBRUTT_SIGNERING),
        () ->
            assertThat(beskjedTilFar.getLink()).isEqualTo(farskapsportalFellesEgenskaper.getUrl()));
  }

  @Test
  void skalVarsleForeldreOmManglendeSigneringForUfoedt() {

    // given
    var noekkelfanger = ArgumentCaptor.forClass(NokkelInput.class);
    var beskjedfanger = ArgumentCaptor.forClass(BeskjedInput.class);
    var erklaeringOpprettetDato = LocalDate.now().minusDays(10);
    var ufoedt = henteBarnUtenFnr(5);

    // when
    brukernotifikasjonConsumer.varsleForeldreOmManglendeSignering(
        MOR, FAR, ufoedt, erklaeringOpprettetDato);

    // then
    verify(beskjedkoe, times(2))
        .send(
            eq(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicBeskjed()),
            noekkelfanger.capture(),
            beskjedfanger.capture());

    var alleBeskjeder = beskjedfanger.getAllValues();
    var beskjedTilMor = alleBeskjeder.get(0);
    var beskjedTilFar = alleBeskjeder.get(1);

    var alleNoekler = noekkelfanger.getAllValues();
    var noekkelTilMor = alleNoekler.get(0);
    var noekkelTilFar = alleNoekler.get(1);

    assertAll(
        () -> assertThat(beskjedTilMor.getEksternVarsling()).isTrue(),
        () -> assertThat(noekkelTilMor.getFodselsnummer()).isEqualTo(MOR.getFoedselsnummer()),
        () ->
            assertThat(noekkelTilMor.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(beskjedTilMor.getSikkerhetsnivaa())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getSikkerhetsnivaaBeskjed()),
        () ->
            assertThat(beskjedTilMor.getTidspunkt())
                .isBetween(
                    Instant.now().minusSeconds(5).toEpochMilli(), Instant.now().toEpochMilli()),
        () ->
            assertThat(beskjedTilMor.getTekst())
                .isEqualTo(
                    String.format(
                        MELDING_OM_MANGLENDE_SIGNERING,
                        erklaeringOpprettetDato.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")),
                        "termindato "
                            + ufoedt
                                .getTermindato()
                                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy")))),
        () ->
            assertThat(beskjedTilMor.getLink()).isEqualTo(farskapsportalFellesEgenskaper.getUrl()));

    assertAll(
        () -> assertThat(beskjedTilFar.getEksternVarsling()).isTrue(),
        () -> assertThat(noekkelTilFar.getFodselsnummer()).isEqualTo(FAR.getFoedselsnummer()),
        () ->
            assertThat(noekkelTilFar.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(beskjedTilFar.getSikkerhetsnivaa())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getSikkerhetsnivaaBeskjed()),
        () ->
            assertThat(beskjedTilFar.getTidspunkt())
                .isBetween(
                    Instant.now().minusSeconds(5).toEpochMilli(), Instant.now().toEpochMilli()),
        () ->
            assertThat(beskjedTilFar.getTekst())
                .isEqualTo(
                    String.format(
                        MELDING_OM_MANGLENDE_SIGNERING,
                        erklaeringOpprettetDato.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")),
                        "termindato "
                            + ufoedt
                                .getTermindato()
                                .format(DateTimeFormatter.ofPattern("dd.MM.yyyy")))),
        () ->
            assertThat(beskjedTilFar.getLink()).isEqualTo(farskapsportalFellesEgenskaper.getUrl()));
  }

  @Test
  void skalVarsleForeldreOmManglendeSigneringForNyfoedt() {

    // given
    var noekkelfanger = ArgumentCaptor.forClass(NokkelInput.class);
    var beskjedfanger = ArgumentCaptor.forClass(BeskjedInput.class);
    var erklaeringOpprettetDato = LocalDate.now().minusDays(10);
    var nyfoedt = henteBarnMedFnr(LocalDate.now().minusMonths(1));

    // when
    brukernotifikasjonConsumer.varsleForeldreOmManglendeSignering(
        MOR, FAR, nyfoedt, LocalDate.now().minusDays(10));

    // then
    verify(beskjedkoe, times(2))
        .send(
            eq(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicBeskjed()),
            noekkelfanger.capture(),
            beskjedfanger.capture());

    var alleBeskjeder = beskjedfanger.getAllValues();
    var beskjedTilMor = alleBeskjeder.get(0);
    var beskjedTilFar = alleBeskjeder.get(1);

    var alleNoekler = noekkelfanger.getAllValues();
    var noekkelTilMor = alleNoekler.get(0);
    var noekkelTilFar = alleNoekler.get(1);

    assertAll(
        () -> assertThat(beskjedTilMor.getEksternVarsling()).isTrue(),
        () -> assertThat(noekkelTilMor.getFodselsnummer()).isEqualTo(MOR.getFoedselsnummer()),
        () ->
            assertThat(noekkelTilMor.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(beskjedTilMor.getSikkerhetsnivaa())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getSikkerhetsnivaaBeskjed()),
        () ->
            assertThat(beskjedTilMor.getTidspunkt())
                .isBetween(
                    Instant.now().minusSeconds(5).toEpochMilli(), Instant.now().toEpochMilli()),
        () ->
            assertThat(beskjedTilMor.getTekst())
                .isEqualTo(
                    String.format(
                        MELDING_OM_MANGLENDE_SIGNERING,
                        erklaeringOpprettetDato.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")),
                        "fødselsnummer " + nyfoedt.getFoedselsnummer())),
        () ->
            assertThat(beskjedTilMor.getLink()).isEqualTo(farskapsportalFellesEgenskaper.getUrl()));

    assertAll(
        () -> assertThat(beskjedTilFar.getEksternVarsling()).isTrue(),
        () -> assertThat(noekkelTilFar.getFodselsnummer()).isEqualTo(FAR.getFoedselsnummer()),
        () ->
            assertThat(noekkelTilFar.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(beskjedTilFar.getSikkerhetsnivaa())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getSikkerhetsnivaaBeskjed()),
        () ->
            assertThat(beskjedTilFar.getTidspunkt())
                .isBetween(
                    Instant.now().minusSeconds(5).toEpochMilli(), Instant.now().toEpochMilli()),
        () ->
            assertThat(beskjedTilFar.getTekst())
                .isEqualTo(
                    String.format(
                        MELDING_OM_MANGLENDE_SIGNERING,
                        erklaeringOpprettetDato.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")),
                        "fødselsnummer " + nyfoedt.getFoedselsnummer())),
        () ->
            assertThat(beskjedTilFar.getLink()).isEqualTo(farskapsportalFellesEgenskaper.getUrl()));
  }

  @Test
  void skalOppretteOppgaveTilFarOmSignering() {

    // given
    oppgavebestillingDao.deleteAll();
    farskapserklaeringDao.deleteAll();

    var dokument =
        Dokument.builder()
            .navn("farskapserklaering.pdf")
            .signeringsinformasjonMor(
                Signeringsinformasjon.builder()
                    .redirectUrl(lageUrl(wiremockPort, "redirect-mor"))
                    .build())
            .signeringsinformasjonFar(
                Signeringsinformasjon.builder()
                    .redirectUrl(lageUrl(wiremockPort, "/redirect-far"))
                    .build())
            .build();

    var farskapserklaeringSomVenterPaaFarsSignatur =
        Farskapserklaering.builder().mor(MOR).far(FAR).barn(BARN).dokument(dokument).build();
    farskapserklaeringSomVenterPaaFarsSignatur
        .getDokument()
        .getSigneringsinformasjonMor()
        .setSigneringstidspunkt(LocalDateTime.now().minusMinutes(3));
    var farskapserklaering =
        persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

    var noekkelfanger = ArgumentCaptor.forClass(NokkelInput.class);
    var oppgavefanger = ArgumentCaptor.forClass(OppgaveInput.class);

    // when
    brukernotifikasjonConsumer.oppretteOppgaveTilFarOmSignering(farskapserklaering.getId(), FAR);

    // then
    verify(oppgavekoe, times(1))
        .send(
            eq(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicOppgave()),
            noekkelfanger.capture(),
            oppgavefanger.capture());

    var nokkel = noekkelfanger.getValue();
    var oppgave = oppgavefanger.getValue();

    var oppgavebestillinger =
        persistenceService.henteAktiveOppgaverTilForelderIFarskapserklaering(
            farskapserklaering.getId(), farskapserklaering.getFar());
    var oppgavebestilling = oppgavebestillinger.stream().findFirst();

    assertAll(
        () -> assertThat(oppgavebestilling).isPresent(),
        () -> assertThat(oppgavebestilling.get().getFerdigstilt()).isNull(),
        () -> assertThat(oppgavebestilling.get().getOpprettet()).isNotNull(),
        () -> assertThat(nokkel.getEventId()).isEqualTo(oppgavebestilling.get().getEventId()),
        () -> assertThat(oppgave.getEksternVarsling()).isTrue(),
        () -> assertThat(nokkel.getFodselsnummer()).isEqualTo(FAR.getFoedselsnummer()),
        () ->
            assertThat(nokkel.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () ->
            assertThat(oppgave.getSikkerhetsnivaa())
                .isEqualTo(Sensitivitet.High),
        () ->
            assertThat(oppgave.getTidspunkt())
                .isBetween(
                    Instant.now().minusSeconds(5).toEpochMilli(), Instant.now().toEpochMilli()),
        () -> assertThat(oppgave.getTekst()).isEqualTo(MELDING_OM_VENTENDE_FARSKAPSERKLAERING),
        () -> assertThat(oppgave.getLink()).isEqualTo(farskapsportalFellesEgenskaper.getUrl()));
  }

  @Test
  void skalIkkeOppretteSigneringsoppgaveDersomEnAlleredeEksistererForFarIFarskapserklaering() {

    // given
    oppgavebestillingDao.deleteAll();
    farskapserklaeringDao.deleteAll();

    var dokument =
        Dokument.builder()
            .navn("farskapserklaering.pdf")
            .signeringsinformasjonMor(
                Signeringsinformasjon.builder()
                    .redirectUrl(lageUrl(wiremockPort, "redirect-mor"))
                    .build())
            .signeringsinformasjonFar(
                Signeringsinformasjon.builder()
                    .redirectUrl(lageUrl(wiremockPort, "/redirect-far"))
                    .build())
            .build();

    var farskapserklaeringSomVenterPaaFarsSignatur =
        Farskapserklaering.builder()
            .mor(henteForelder(Forelderrolle.MOR))
            .far(henteForelder(Forelderrolle.FAR))
            .barn(henteBarnUtenFnr(4))
            .dokument(dokument)
            .build();
    farskapserklaeringSomVenterPaaFarsSignatur
        .getDokument()
        .getSigneringsinformasjonMor()
        .setSigneringstidspunkt(LocalDateTime.now().minusMinutes(3));
    var farskapserklaering =
        persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

    oppgavebestillingDao.save(
        Oppgavebestilling.builder()
            .forelder(farskapserklaering.getFar())
            .farskapserklaering(farskapserklaering)
            .opprettet(LocalDateTime.now())
            .eventId(UUID.randomUUID().toString())
            .build());

    // when
    brukernotifikasjonConsumer.oppretteOppgaveTilFarOmSignering(farskapserklaering.getId(), FAR);

    // then
    verify(oppgavekoe, times(0)).send(anyString(), any(NokkelInput.class), any(OppgaveInput.class));
    var oppgavebestillinger =
        persistenceService.henteAktiveOppgaverTilForelderIFarskapserklaering(
            farskapserklaering.getId(), farskapserklaering.getFar());
    var oppgavebestilling = oppgavebestillinger.stream().findFirst();

    assertAll(
        () -> assertThat(oppgavebestilling).isPresent(),
        () -> assertThat(oppgavebestilling.get().getFerdigstilt()).isNull(),
        () -> assertThat(oppgavebestilling.get().getOpprettet()).isNotNull());
  }

  @Test
  void skalSletteFarsSigneringsoppgave() {

    // given
    oppgavebestillingDao.deleteAll();
    farskapserklaeringDao.deleteAll();

    var dokument =
        Dokument.builder()
            .navn("farskapserklaering.pdf")
            .signeringsinformasjonMor(
                Signeringsinformasjon.builder()
                    .redirectUrl(lageUrl(wiremockPort, "redirect-mor"))
                    .build())
            .signeringsinformasjonFar(
                Signeringsinformasjon.builder()
                    .redirectUrl(lageUrl(wiremockPort, "/redirect-far"))
                    .build())
            .build();
    var farskapserklaeringSomVenterPaaFarsSignatur =
        Farskapserklaering.builder()
            .mor(henteForelder(Forelderrolle.MOR))
            .far(henteForelder(Forelderrolle.FAR))
            .barn(TestUtils.henteBarnUtenFnr(5))
            .dokument(dokument)
            .build();
    farskapserklaeringSomVenterPaaFarsSignatur
        .getDokument()
        .getSigneringsinformasjonMor()
        .setSigneringstidspunkt(LocalDateTime.now().minusMinutes(3));

    var farskapserklaering =
        persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

    var noekkelfanger = ArgumentCaptor.forClass(NokkelInput.class);
    var ferdigfanger = ArgumentCaptor.forClass(DoneInput.class);

    var eksisterendeOppgavebestilling =
        persistenceService.lagreNyOppgavebestilling(
            farskapserklaering.getId(), UUID.randomUUID().toString());

    // when
    brukernotifikasjonConsumer.sletteFarsSigneringsoppgave(
        eksisterendeOppgavebestilling.getEventId(), FAR);

    // then
    verify(ferdigkoe, times(1))
        .send(
            eq(farskapsportalFellesEgenskaper.getBrukernotifikasjon().getTopicFerdig()),
            noekkelfanger.capture(),
            ferdigfanger.capture());

    var nokkel = noekkelfanger.getAllValues().get(0);
    var ferdig = ferdigfanger.getAllValues().get(0);

    assertAll(
        () ->
            assertThat(nokkel.getGrupperingsId())
                .isEqualTo(
                    farskapsportalFellesEgenskaper
                        .getBrukernotifikasjon()
                        .getGrupperingsidFarskap()),
        () -> assertThat(nokkel.getFodselsnummer()).isEqualTo(FAR.getFoedselsnummer()),
        () ->
            assertThat(ferdig.getTidspunkt())
                .isGreaterThan(Instant.now().minusSeconds(10).toEpochMilli()));
  }
}

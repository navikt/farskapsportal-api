package no.nav.farskapsportal.service;

import static no.nav.farskapsportal.FarskapsportalApplicationLocal.PROFILE_TEST;
import static no.nav.farskapsportal.TestUtils.henteBarnMedFnr;
import static no.nav.farskapsportal.TestUtils.henteBarnUtenFnr;
import static no.nav.farskapsportal.TestUtils.henteFarskapserklaeringDto;
import static no.nav.farskapsportal.TestUtils.henteForelder;
import static no.nav.farskapsportal.api.Feilkode.ERKLAERING_EKSISTERER_BARN;
import static no.nav.farskapsportal.api.Feilkode.ERKLAERING_EKSISTERER_MOR;
import static no.nav.farskapsportal.api.Feilkode.FANT_IKKE_FARSKAPSERKLAERING;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import no.nav.farskapsportal.FarskapsportalApplicationLocal;
import no.nav.farskapsportal.api.Feilkode;
import no.nav.farskapsportal.api.Forelderrolle;
import no.nav.farskapsportal.config.egenskaper.FarskapsportalEgenskaper;
import no.nav.farskapsportal.consumer.pdl.api.KjoennType;
import no.nav.farskapsportal.consumer.pdl.api.NavnDto;
import no.nav.farskapsportal.dto.BarnDto;
import no.nav.farskapsportal.dto.FarskapserklaeringDto;
import no.nav.farskapsportal.dto.ForelderDto;
import no.nav.farskapsportal.exception.InternFeilException;
import no.nav.farskapsportal.exception.RessursIkkeFunnetException;
import no.nav.farskapsportal.exception.ValideringException;
import no.nav.farskapsportal.persistence.dao.DokumentDao;
import no.nav.farskapsportal.persistence.dao.FarskapserklaeringDao;
import no.nav.farskapsportal.persistence.dao.ForelderDao;
import no.nav.farskapsportal.persistence.dao.StatusKontrollereFarDao;
import no.nav.farskapsportal.persistence.entity.Dokument;
import no.nav.farskapsportal.persistence.entity.Farskapserklaering;
import no.nav.farskapsportal.persistence.entity.Forelder;
import no.nav.farskapsportal.persistence.entity.Signeringsinformasjon;
import no.nav.farskapsportal.persistence.entity.StatusKontrollereFar;
import no.nav.farskapsportal.util.Mapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ActiveProfiles;

@DisplayName("PersistenceServiceTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = FarskapsportalApplicationLocal.class)
@ActiveProfiles(PROFILE_TEST)
public class PersistenceServiceTest {

  private static final ForelderDto MOR = henteForelder(Forelderrolle.MOR);
  private static final ForelderDto FAR = henteForelder(Forelderrolle.FAR);
  private static final NavnDto NAVN_MOR = NavnDto.builder().fornavn(MOR.getNavn().getFornavn()).etternavn(MOR.getNavn().getEtternavn()).build();
  private static final NavnDto NAVN_FAR = NavnDto.builder().fornavn(FAR.getNavn().getFornavn()).etternavn(FAR.getNavn().getEtternavn()).build();
  private static final BarnDto UFOEDT_BARN = henteBarnUtenFnr(17);
  private static final BarnDto NYFOEDT_BARN = henteBarnMedFnr(LocalDate.now().minusWeeks(2));
  private static final FarskapserklaeringDto FARSKAPSERKLAERING = henteFarskapserklaeringDto(MOR, FAR, UFOEDT_BARN);

  @MockBean
  private PersonopplysningService personopplysningServiceMock;
  @Autowired
  private PersistenceService persistenceService;
  @Autowired
  private FarskapserklaeringDao farskapserklaeringDao;
  @Autowired
  private ForelderDao forelderDao;
  @Autowired
  private DokumentDao dokumentDao;
  @Autowired
  private StatusKontrollereFarDao statusKontrollereFarDao;
  @Autowired
  private Mapper mapper;
  @Autowired
  private FarskapsportalEgenskaper farskapsportalEgenskaper;

  @Test
  void skalSetteAntallFeiledeForseokTilEnDersomTidspunktForNullstillingErNaadd() {

  }

  private void standardPersonopplysningerMocks(ForelderDto far, ForelderDto mor) {
    when(personopplysningServiceMock.henteNavn(far.getFoedselsnummer())).thenReturn(NAVN_FAR);
    when(personopplysningServiceMock.henteFoedselsdato(far.getFoedselsnummer())).thenReturn(FAR.getFoedselsdato());

    when(personopplysningServiceMock.henteNavn(mor.getFoedselsnummer())).thenReturn(NAVN_MOR);
    when(personopplysningServiceMock.henteFoedselsdato(mor.getFoedselsnummer())).thenReturn(MOR.getFoedselsdato());
    when(personopplysningServiceMock.harNorskBostedsadresse(mor.getFoedselsnummer())).thenReturn(true);
  }

  @Nested
  @DisplayName("Lagre")
  @DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
  @AutoConfigureTestDatabase(replace = Replace.ANY)
  class Lagre {

    @Test
    @DisplayName("Lagre dokument")
    void lagreDokument() {

      // given
      var redirectUrlMor = "https://esignering.no/redirect-mor";
      var redirectUrlFar = "https://esignering.no/redirect-far";

      var dokument = Dokument.builder().navn("farskapserklaring.pdf")
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl(redirectUrlMor).build())
          .signeringsinformasjonFar(Signeringsinformasjon.builder().redirectUrl(redirectUrlFar).build()).build();

      // when
      var lagretDokument = dokumentDao.save(dokument);
      var hentetDokument = dokumentDao.findById(lagretDokument.getId()).get();

      // then
      assertEquals(dokument.getNavn(), hentetDokument.getNavn());

      // clean up test data
      dokumentDao.delete(lagretDokument);
    }

    @Test
    @DisplayName("Lagre farskapserklæring")
    void lagreFarskapserklaering() {

      // given
      statusKontrollereFarDao.deleteAll();
      farskapserklaeringDao.deleteAll();

      // when
      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(mapper.toEntity(FARSKAPSERKLAERING));

      var hentetFarskapserklaering = farskapserklaeringDao.findById(lagretFarskapserklaering.getId()).get();

      // then
      assertEquals(lagretFarskapserklaering, hentetFarskapserklaering, "Farskapserklæringen som ble lagret er lik den som ble hentet");

      // rydde test data
      farskapserklaeringDao.delete(lagretFarskapserklaering);
    }

    @Test
    void lagreFarskapserklaeringMedSammeMorFarOgBarnSomIDeaktivertFarskapserklaering() {

      // given
      statusKontrollereFarDao.deleteAll();
      farskapserklaeringDao.deleteAll();

      var deaktivertFarskapserklaeringMedSammeMorFarOgBarn = henteFarskapserklaeringDto(MOR, FAR, NYFOEDT_BARN);
      var lagretDeaktivertFarskapserklaering = persistenceService.lagreNyFarskapserklaering(
          mapper.toEntity(deaktivertFarskapserklaeringMedSammeMorFarOgBarn));
      lagretDeaktivertFarskapserklaering.setDeaktivert(LocalDateTime.now());
      persistenceService.oppdatereFarskapserklaering(lagretDeaktivertFarskapserklaering);

      var duplikatAktivFarskapserklaering = henteFarskapserklaeringDto(MOR, FAR, NYFOEDT_BARN);

      // when
      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(mapper.toEntity(duplikatAktivFarskapserklaering));

      var hentetFarskapserklaering = farskapserklaeringDao.findById(lagretFarskapserklaering.getId()).get();

      // then
      assertEquals(lagretFarskapserklaering, hentetFarskapserklaering, "Farskapserklæringen som ble lagret er lik den som ble hentet");

      // rydde test data
      farskapserklaeringDao.delete(lagretFarskapserklaering);

    }

    @Test
    @DisplayName("Skal ikke lagre ny farskapserklæring dersom tilsvarende allerede eksisterer")
    void skalIkkeLagreNyFarskapserklaeringDersomTilsvarendeAlleredeEksisterer() {

      // rydde testdata
      statusKontrollereFarDao.deleteAll();
      farskapserklaeringDao.deleteAll();
      forelderDao.deleteAll();

      // given
      farskapserklaeringDao.save(mapper.toEntity(FARSKAPSERKLAERING));

      // when, then
      assertThrows(ValideringException.class, () -> persistenceService.lagreNyFarskapserklaering(mapper.toEntity(FARSKAPSERKLAERING)));
    }

    @Test
    @DisplayName("Skal lagre ny instans av StatusKontrollereFar")
    void skalLagreNyInstansAvStatuskontrollereFar() {

      // rydde testdata
      statusKontrollereFarDao.deleteAll();
      farskapserklaeringDao.deleteAll();

      // given
      var antallDagerTilNullsettingAvForsoek = 1;
      var navnDtoMor = NavnDto.builder().fornavn(MOR.getNavn().getFornavn()).etternavn(MOR.getNavn().getEtternavn()).build();
      when(personopplysningServiceMock.henteNavn(MOR.getFoedselsnummer())).thenReturn(navnDtoMor);

      // when
      var lagretStatusKontrollereFar = persistenceService.oppdatereStatusKontrollereFar(MOR.getFoedselsnummer(), antallDagerTilNullsettingAvForsoek,
          farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek());

      var hentetStatusKontrollereFar = statusKontrollereFarDao.findById(lagretStatusKontrollereFar.getId());

      // then
      assertAll(
          () -> assertThat(lagretStatusKontrollereFar.getAntallFeiledeForsoek())
              .isEqualTo(hentetStatusKontrollereFar.get().getAntallFeiledeForsoek()),
          () -> assertThat(lagretStatusKontrollereFar.getTidspunktForNullstilling())
              .isEqualToIgnoringSeconds(hentetStatusKontrollereFar.get().getTidspunktForNullstilling()),
          () -> assertThat(lagretStatusKontrollereFar.getMor().getFoedselsnummer())
              .isEqualTo(hentetStatusKontrollereFar.get().getMor().getFoedselsnummer()));
    }
  }

  @Nested
  @DisplayName("Hente")
  @TestInstance(Lifecycle.PER_CLASS)
  class Hente {

    private Farskapserklaering lagreFarskapserklaering() {
      statusKontrollereFarDao.deleteAll();
      farskapserklaeringDao.deleteAll();
      forelderDao.deleteAll();
      return farskapserklaeringDao.save(mapper.toEntity(FARSKAPSERKLAERING));
    }

    void lagreFarskapserklaeringSignertAvMor() {
      var farskapserklaeringSignertAvMor = lagreFarskapserklaering();
      farskapserklaeringSignertAvMor.getDokument().setPadesUrl("https://esignering.posten.no/" + MOR.getFoedselsnummer() + "/pades");
      farskapserklaeringSignertAvMor.getDokument().getSigneringsinformasjonMor().setSigneringstidspunkt(LocalDateTime.now().minusMinutes(15));
      farskapserklaeringDao.save(farskapserklaeringSignertAvMor);
    }

    @Test
    @DisplayName("Skal hente farskapserklæring i forbindelse med mors redirect fra signeringsløsningen")
    void skalHenteFarskapserklaeringEtterRedirectForMor() {

      // given
      var lagretFarskapserklaering = lagreFarskapserklaering();

      assertAll(() -> assertNull(lagretFarskapserklaering.getDokument().getPadesUrl()),
          () -> assertNull(lagretFarskapserklaering.getDokument().getSigneringsinformasjonMor().getSigneringstidspunkt()),
          () -> assertNull(lagretFarskapserklaering.getDokument().getSigneringsinformasjonFar().getSigneringstidspunkt()));

      // when
      var farskapserklaeringerEtterRedirect = persistenceService
          .henteFarskapserklaeringerEtterRedirect(MOR.getFoedselsnummer(), Forelderrolle.MOR, KjoennType.KVINNE).stream().findFirst().get();

      // then
      assertAll(
          () -> assertNull(farskapserklaeringerEtterRedirect.getDokument().getPadesUrl(),
              "PAdES-URL skal ikke være satt i farskapserklæring i det mor redirektes tilbake til farskapsportalen etter utført signering"),
          () -> assertEquals(FARSKAPSERKLAERING.getMor().getFoedselsnummer(), farskapserklaeringerEtterRedirect.getMor().getFoedselsnummer()),
          () -> assertEquals(FARSKAPSERKLAERING.getFar().getFoedselsnummer(), farskapserklaeringerEtterRedirect.getFar().getFoedselsnummer()),
          () -> assertEquals(FARSKAPSERKLAERING.getBarn().getTermindato(), farskapserklaeringerEtterRedirect.getBarn().getTermindato()),
          () -> assertThat(farskapserklaeringerEtterRedirect.getDeaktivert()).isNull()
      );
    }

    @Test
    @DisplayName("Skal hente farskapserklæring i forbindelse med fars redirect fra signeringsløsningen")
    void skalHenteFarskapserklaeringEtterRedirectForFar() {

      // given
      lagreFarskapserklaeringSignertAvMor();

      //when
      var farskapserklaeringerEtterRedirect = persistenceService
          .henteFarskapserklaeringerEtterRedirect(FAR.getFoedselsnummer(), Forelderrolle.FAR, KjoennType.MANN).stream().findFirst().get();

      // then
      assertAll(
          () -> assertNotNull(farskapserklaeringerEtterRedirect.getDokument(),
              "PAdES-URL skal være satt i farskapserklæring i det far redirektes tilbake til farskapsportalen etter utført signering"),
          () -> assertEquals(FARSKAPSERKLAERING.getMor().getFoedselsnummer(), farskapserklaeringerEtterRedirect.getMor().getFoedselsnummer()),
          () -> assertEquals(FARSKAPSERKLAERING.getFar().getFoedselsnummer(), farskapserklaeringerEtterRedirect.getFar().getFoedselsnummer()),
          () -> assertEquals(FARSKAPSERKLAERING.getBarn().getTermindato(), farskapserklaeringerEtterRedirect.getBarn().getTermindato()),
          () -> assertThat(farskapserklaeringerEtterRedirect.getDeaktivert()).isNull()
      );
    }

    @Test
    @DisplayName("Skal hente farskapserklæring som venter på far")
    void skalHenteFarskapserklaeringSomVenterPaaFar() {

      // given
      lagreFarskapserklaeringSignertAvMor();

      standardPersonopplysningerMocks(FAR, MOR);

      // when
      var hentedeFarskapserklaeringer = persistenceService.henteFarsErklaeringer(FAR.getFoedselsnummer());

      // then
      var hentetFarskapserklaering = hentedeFarskapserklaeringer.stream().filter(f -> FAR.getFoedselsnummer().equals(f.getFar().getFoedselsnummer()))
          .findFirst().get();

      assertAll(
          () -> assertEquals(FARSKAPSERKLAERING.getFar().getFoedselsnummer(), hentetFarskapserklaering.getFar().getFoedselsnummer()),
          () -> assertEquals(FARSKAPSERKLAERING.getBarn().getTermindato(), hentetFarskapserklaering.getBarn().getTermindato()),
          () -> assertThat(hentetFarskapserklaering.getDeaktivert()).isNull()
      );
    }

    @Test
    @DisplayName("Skal hente lagret barn")
    void skalHenteLagretBarn() {

      // given
      var lagretFarskapserklaering = lagreFarskapserklaering();

      // when
      var hentetBarn = persistenceService.henteBarn(lagretFarskapserklaering.getBarn().getId());

      // then
      assertEquals(lagretFarskapserklaering.getBarn().getTermindato(), hentetBarn.getTermindato());
    }

    @Test
    @DisplayName("Skal hente lagret mor")
    void skalHenteLagretMor() {

      // given
      var lagretFarskapserklaering = lagreFarskapserklaering();

      // when
      var hentetMor = persistenceService.henteForelder(lagretFarskapserklaering.getMor().getId());

      // then
      assertEquals(lagretFarskapserklaering.getMor().getFoedselsnummer(), hentetMor.getFoedselsnummer());
    }

    @Test
    @DisplayName("Skal hente lagret far")
    void skalHenteLagretFar() {

      // given
      var lagretFarskapserklaering = lagreFarskapserklaering();

      // when
      var hentetFar = persistenceService.henteForelder(lagretFarskapserklaering.getFar().getId());

      // then
      assertEquals(lagretFarskapserklaering.getFar().getFoedselsnummer(), hentetFar.getFoedselsnummer());
    }

    @Test
    @DisplayName("Skal hente lagret statusKontrollereFar")
    void skalHenteLagretStatusKontrollereFar() {

      // given
      var antallDagerTilNullsettingAvForsoek = 1;
      var foerTidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullsettingAvForsoek);
      persistenceService.oppdatereStatusKontrollereFar(MOR.getFoedselsnummer(), antallDagerTilNullsettingAvForsoek,
          farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek());

      // when
      var hentetStatusLagreKontrollereFar = persistenceService.henteStatusKontrollereFar(MOR.getFoedselsnummer());

      // then
      var etterTidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullsettingAvForsoek);

      assertAll(
          () -> assertThat(hentetStatusLagreKontrollereFar).isPresent(),
          () -> assertThat(hentetStatusLagreKontrollereFar.get().getTidspunktForNullstilling()).isBefore(etterTidspunktForNullstilling),
          () -> assertThat(hentetStatusLagreKontrollereFar.get().getTidspunktForNullstilling()).isAfter(foerTidspunktForNullstilling)
      );
    }

    @Test
    void skalHenteFarskapserklaeringForIdSomFinnesIDatabasen() {

      // given
      var lagretFarskapserklaering = lagreFarskapserklaering();
      assertNotNull(lagretFarskapserklaering);

      // when
      var farskapserklaering = persistenceService.henteFarskapserklaeringForId(lagretFarskapserklaering.getId());

      // then
      assertAll(
          () -> assertThat(lagretFarskapserklaering.getDokument().getSigneringsinformasjonFar().getUndertegnerUrl())
              .isEqualTo(farskapserklaering.getDokument().getSigneringsinformasjonFar().getUndertegnerUrl()),
          () -> assertThat(lagretFarskapserklaering.getDeaktivert()).isNull()
      );
    }

    @Test
    @DisplayName("Skal kaste RessursIkkeFunnetException ved henting av undertegnerUrl dersom farskapserklaering ikke finnes")
    void skalKasteRessursIkkeFunnetExceptionVedHentingAvUndertegnerurlDersomFarskapserklaeringIkkeFinnes() {

      // given
      var lagretFarskapserklaering = lagreFarskapserklaering();
      assertNotNull(lagretFarskapserklaering);

      // when, then
      assertThrows(RessursIkkeFunnetException.class, () -> persistenceService.henteFarskapserklaeringForId(lagretFarskapserklaering.getId() + 1));
    }
  }

  @Nested
  @DisplayName("Slette")
  class Slette {

    private Farskapserklaering lagreFarskapserklaering() {
      statusKontrollereFarDao.deleteAll();
      farskapserklaeringDao.deleteAll();
      forelderDao.deleteAll();
      return farskapserklaeringDao.save(mapper.toEntity(FARSKAPSERKLAERING));
    }

    @Test
    void skalDeaktivereFarskapserklaeringSomManglerFarsSignatur() {

      // given
      var lagretFarskapserklaering = lagreFarskapserklaering();

      // when
      persistenceService.deaktivereFarskapserklaering(lagretFarskapserklaering.getId());

      // then
      assertThrows(RessursIkkeFunnetException.class, () -> persistenceService.henteFarskapserklaeringForId(lagretFarskapserklaering.getId()));
    }

    @Test
    void skalKasteInternFeilExceptionDersomFarskapserklaeringBlirForsoektDeaktivertIkkeFinnes() {

      // given
      var lagretFarskapserklaering = lagreFarskapserklaering();

      // when
      var internFeilException = assertThrows(InternFeilException.class,
          () -> persistenceService.deaktivereFarskapserklaering(lagretFarskapserklaering.getId() + 1));

      // then
      assertThat(internFeilException.getFeilkode()).isEqualTo(FANT_IKKE_FARSKAPSERKLAERING);
    }
  }

  @Nested
  @DisplayName("OppdatereStatusKontrollereFar")
  class OppdatereStatusKontrollereFar {

    @BeforeEach
    void ryddeTestdata() {
      statusKontrollereFarDao.deleteAll();
      farskapserklaeringDao.deleteAll();
      forelderDao.deleteAll();
    }

    @Test
    void skalOppretteNyOppdatereStatusKontrollereFarDersomKontrollFarFeilerForFoersteGang() {

      // given
      var antallDagerTilNullstilling = 1;
      var tidspunktForNullstillingFoerLogging = LocalDateTime.now().plusDays(antallDagerTilNullstilling);
      forelderDao.save(Forelder.builder().foedselsnummer(MOR.getFoedselsnummer()).build());

      // when
      var statusKontrollereFar = persistenceService.oppdatereStatusKontrollereFar(MOR.getFoedselsnummer(), antallDagerTilNullstilling,
          farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek());

      // then
      var tidspunktForNullstillingEtterLogging = LocalDateTime.now().plusDays(antallDagerTilNullstilling);

      assertAll(
          () -> assertThat(statusKontrollereFar.getAntallFeiledeForsoek()).isEqualTo(1),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isAfter(tidspunktForNullstillingFoerLogging),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isBefore(tidspunktForNullstillingEtterLogging),
          () -> assertThat(statusKontrollereFar.getMor().getFoedselsnummer()).isEqualTo(MOR.getFoedselsnummer()));
    }

    @Test
    void skalLeggeInnMorSomForelderDersomHunIkkeEksistererIDatabasenVedOpprettelseAvNyttInnslagIStatusKontrollereFar() {

      // given
      var antallDagerTilNullstilling = 1;
      var foerTidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullstilling);
      var navnDtoMor = NavnDto.builder().fornavn(MOR.getNavn().getFornavn()).etternavn(MOR.getNavn().getEtternavn()).build();
      when(personopplysningServiceMock.henteNavn(MOR.getFoedselsnummer())).thenReturn(navnDtoMor);

      // when
      var statusKontrollereFar = persistenceService.oppdatereStatusKontrollereFar(MOR.getFoedselsnummer(), antallDagerTilNullstilling,
          farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek());

      // then
      var etterTidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullstilling);

      assertAll(
          () -> assertThat(statusKontrollereFar.getAntallFeiledeForsoek()).isEqualTo(1),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isAfter(foerTidspunktForNullstilling),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isBefore(etterTidspunktForNullstilling),
          () -> assertThat(statusKontrollereFar.getMor().getFoedselsnummer()).isEqualTo(MOR.getFoedselsnummer()));
    }

    @Test
    void skalReferereTilRiktigForelder() {

      // given
      var antallDagerTilNullstilling = 1;
      var foerTidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullstilling);
      var navnDtoMor = NavnDto.builder().fornavn(MOR.getNavn().getFornavn()).etternavn(MOR.getNavn().getEtternavn()).build();
      when(personopplysningServiceMock.henteNavn(MOR.getFoedselsnummer())).thenReturn(navnDtoMor);
      forelderDao.save(Forelder.builder().foedselsnummer(MOR.getFoedselsnummer()).build());

      // when
      var statusKontrollereFar = persistenceService.oppdatereStatusKontrollereFar(MOR.getFoedselsnummer(), antallDagerTilNullstilling,
          farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek());

      // then
      var etterTidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullstilling);

      assertAll(
          () -> assertThat(statusKontrollereFar.getAntallFeiledeForsoek()).isEqualTo(1),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isAfter(foerTidspunktForNullstilling),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isBefore(etterTidspunktForNullstilling),
          () -> assertThat(statusKontrollereFar.getMor().getFoedselsnummer()).isEqualTo(MOR.getFoedselsnummer()));
    }

    @Test
    void skalIkkeOppdatereDersomAntallForsoekErOverskredetOgFornyelseperiodeIkkeUtloept() {

      // given
      var antallDagerTilNullstilling = 1;
      var foerTidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullstilling);
      var navnDtoMor = NavnDto.builder().fornavn(MOR.getNavn().getFornavn()).etternavn(MOR.getNavn().getEtternavn()).build();
      when(personopplysningServiceMock.henteNavn(MOR.getFoedselsnummer())).thenReturn(navnDtoMor);
      forelderDao.save(Forelder.builder().foedselsnummer(MOR.getFoedselsnummer()).build());

      // when
      for (int i = 1; i <= farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek(); i++) {
        var statusKontrollereFar = persistenceService.oppdatereStatusKontrollereFar(MOR.getFoedselsnummer(), antallDagerTilNullstilling,
            farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek());
        int finalI = i;
        assertAll(
            () -> assertThat(statusKontrollereFar.getAntallFeiledeForsoek()).isEqualTo(finalI),
            () -> assertThat(statusKontrollereFar.getMor().getFoedselsnummer()).isEqualTo(MOR.getFoedselsnummer())
        );
      }

      var statusKontrollereFar = persistenceService.oppdatereStatusKontrollereFar(MOR.getFoedselsnummer(), antallDagerTilNullstilling,
          farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek());

      // then
      var etterTidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullstilling);

      assertAll(
          () -> assertThat(statusKontrollereFar.getAntallFeiledeForsoek()).isEqualTo(farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek()),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isAfter(foerTidspunktForNullstilling),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isBefore(etterTidspunktForNullstilling),
          () -> assertThat(statusKontrollereFar.getMor().getFoedselsnummer()).isEqualTo(MOR.getFoedselsnummer()));
    }

    @Test
    void skalInkrementereAntallFeiledeForsoekDersomTidspunktForNullstillingIkkeErNaadd() {

      // given
      var antallDagerTilNullstilling = 1;
      var tidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullstilling);
      var eksisterendeStatusKontrollereFar = lagreStatusKontrollereFarMedMor(farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek() - 1,
          tidspunktForNullstilling);

      // when
      var statusKontrollereFar = persistenceService.oppdatereStatusKontrollereFar(MOR.getFoedselsnummer(), antallDagerTilNullstilling,
          farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek());

      // then
      var etterTidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullstilling);

      assertAll(
          () -> assertThat(statusKontrollereFar.getAntallFeiledeForsoek()).isEqualTo(eksisterendeStatusKontrollereFar.getAntallFeiledeForsoek() + 1),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isBefore(etterTidspunktForNullstilling),
          () -> assertThat(statusKontrollereFar.getMor().getFoedselsnummer()).isEqualTo(MOR.getFoedselsnummer()));
    }

    @Test
    void skalSetteAntallForsoekTilEnVedFeilDersomTidspunktForNullstillingErNaadd() {

      // given
      var antallDagerTilNullstilling = 0;
      var tidspunktForNullstilling = LocalDateTime.now().plusDays(antallDagerTilNullstilling);
      lagreStatusKontrollereFarMedMor(farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek() - 1, tidspunktForNullstilling);

      // when
      var statusKontrollereFar = persistenceService.oppdatereStatusKontrollereFar(MOR.getFoedselsnummer(), antallDagerTilNullstilling,
          farskapsportalEgenskaper.getKontrollFarMaksAntallForsoek());

      // then
      var tidspunktEtterLogging = LocalDateTime.now();

      assertAll(
          () -> assertThat(statusKontrollereFar.getAntallFeiledeForsoek()).isEqualTo(1),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isAfter(tidspunktForNullstilling),
          () -> assertThat(statusKontrollereFar.getTidspunktForNullstilling()).isBefore(tidspunktEtterLogging),
          () -> assertThat(statusKontrollereFar.getMor().getFoedselsnummer()).isEqualTo(MOR.getFoedselsnummer()));
    }

    private StatusKontrollereFar lagreStatusKontrollereFarMedMor(int antallFeil, LocalDateTime tidspunktForNullstilling) {
      return statusKontrollereFarDao.save(StatusKontrollereFar.builder().mor(mapper.toEntity(MOR)).antallFeiledeForsoek(antallFeil)
          .tidspunktForNullstilling(tidspunktForNullstilling).build());
    }
  }

  @Nested
  @DisplayName("IngenKonfliktMedEksisterendeFarskapserklaeringer")
  class IngenKonfliktMedEksisterendeFarskapserklaeringer {

    @BeforeEach
    void ryddeTestdata() {
      statusKontrollereFarDao.deleteAll();
      farskapserklaeringDao.deleteAll();
    }

    @Test
    void skalKasteValideringExceptionDersomMorHarEksisterendeFarskapserklaeringOgOppretterNyMedTermindato() {

      // given
      persistenceService.lagreNyFarskapserklaering(mapper.toEntity(FARSKAPSERKLAERING));

      // when, then
      var valideringException = assertThrows(ValideringException.class,
          () -> persistenceService.ingenKonfliktMedEksisterendeFarskapserklaeringer(MOR.getFoedselsnummer(), FAR.getFoedselsnummer(), UFOEDT_BARN));

      // then
      assertThat(valideringException.getFeilkode()).isEqualTo(ERKLAERING_EKSISTERER_MOR);
    }

    @Test
    void skalKasteValideringExceptionDersomNyfoedtBarnInngaarIEksisterendeFarskapserklaering() {

      // given
      var fnrMorUtenEksisterendeFarskapserklaering = LocalDate.now().minusYears(29).format(DateTimeFormatter.ofPattern("ddMMyy")) + "12245";
      var farskapserklaering = FarskapserklaeringDto.builder().barn(NYFOEDT_BARN).mor(MOR).far(FAR).dokument(FARSKAPSERKLAERING.getDokument())
          .build();
      persistenceService.lagreNyFarskapserklaering(mapper.toEntity(farskapserklaering));

      // when
      var valideringException = assertThrows(ValideringException.class, () -> persistenceService
          .ingenKonfliktMedEksisterendeFarskapserklaeringer(fnrMorUtenEksisterendeFarskapserklaering, FAR.getFoedselsnummer(), NYFOEDT_BARN));

      // then
      assertThat(valideringException.getFeilkode()).isEqualTo(ERKLAERING_EKSISTERER_BARN);
    }

    @Test
    void skalIkkeKasteExceptionDersomMorIkkeHarEksisterendeFarskapserklaering() {

      // given, when, then
      assertDoesNotThrow(
          () -> persistenceService.ingenKonfliktMedEksisterendeFarskapserklaeringer(MOR.getFoedselsnummer(), FAR.getFoedselsnummer(), UFOEDT_BARN));
    }

    @Test
    void skalIkkeKasteExceptionDersomMorHarEnEksisterendeDeaktivertFarskapserklaering() {

      // given
      var deaktivertFarskapserklaering = persistenceService.lagreNyFarskapserklaering(mapper.toEntity(FARSKAPSERKLAERING));
      deaktivertFarskapserklaering.setDeaktivert(LocalDateTime.now());
      persistenceService.oppdatereFarskapserklaering(deaktivertFarskapserklaering);

      // when, then
      var ressursIkkeFunnetException = assertThrows(RessursIkkeFunnetException.class,
          () -> persistenceService.henteFarskapserklaeringForId(deaktivertFarskapserklaering.getId()));

      assertThat(ressursIkkeFunnetException.getFeilkode()).isEqualTo(Feilkode.FANT_IKKE_FARSKAPSERKLAERING);

      assertDoesNotThrow(
          () -> persistenceService.ingenKonfliktMedEksisterendeFarskapserklaeringer(MOR.getFoedselsnummer(), FAR.getFoedselsnummer(), UFOEDT_BARN));

    }
  }
}

package no.nav.farskapsportal.service;

import static no.nav.farskapsportal.FarskapsportalApplicationLocal.PROFILE_TEST;
import static no.nav.farskapsportal.TestUtils.FOEDSELSDATO_FAR;
import static no.nav.farskapsportal.TestUtils.FOEDSELSDATO_MOR;
import static no.nav.farskapsportal.TestUtils.henteBarnMedFnr;
import static no.nav.farskapsportal.TestUtils.henteBarnUtenFnr;
import static no.nav.farskapsportal.TestUtils.henteFarskapserklaering;
import static no.nav.farskapsportal.TestUtils.henteForelder;
import static no.nav.farskapsportal.TestUtils.henteNyligFoedtBarn;
import static no.nav.farskapsportal.TestUtils.lageUrl;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import lombok.SneakyThrows;
import no.nav.farskapsportal.FarskapsportalApplicationLocal;
import no.nav.farskapsportal.api.Forelderrolle;
import no.nav.farskapsportal.api.KontrollerePersonopplysningerRequest;
import no.nav.farskapsportal.api.OppdatereFarskapserklaeringRequest;
import no.nav.farskapsportal.api.OppretteFarskapserklaeringRequest;
import no.nav.farskapsportal.api.Rolle;
import no.nav.farskapsportal.api.Sivilstandtype;
import no.nav.farskapsportal.config.FarskapsportalEgenskaper;
import no.nav.farskapsportal.consumer.esignering.DifiESignaturConsumer;
import no.nav.farskapsportal.consumer.esignering.api.DokumentStatusDto;
import no.nav.farskapsportal.consumer.esignering.api.SignaturDto;
import no.nav.farskapsportal.consumer.pdf.PdfGeneratorConsumer;
import no.nav.farskapsportal.consumer.pdl.api.KjoennDto;
import no.nav.farskapsportal.consumer.pdl.api.KjoennType;
import no.nav.farskapsportal.consumer.pdl.api.NavnDto;
import no.nav.farskapsportal.consumer.pdl.api.SivilstandDto;
import no.nav.farskapsportal.consumer.skatt.SkattConsumer;
import no.nav.farskapsportal.dto.BarnDto;
import no.nav.farskapsportal.dto.ForelderDto;
import no.nav.farskapsportal.exception.FeilNavnOppgittException;
import no.nav.farskapsportal.exception.ManglerRelasjonException;
import no.nav.farskapsportal.exception.MorHarIngenNyfoedteUtenFarException;
import no.nav.farskapsportal.exception.NyfoedtErForGammelException;
import no.nav.farskapsportal.exception.RessursIkkeFunnetException;
import no.nav.farskapsportal.exception.ValideringException;
import no.nav.farskapsportal.persistence.dao.FarskapserklaeringDao;
import no.nav.farskapsportal.persistence.dao.ForelderDao;
import no.nav.farskapsportal.persistence.dao.StatusKontrollereFarDao;
import no.nav.farskapsportal.persistence.entity.Dokument;
import no.nav.farskapsportal.persistence.entity.Dokumentinnhold;
import no.nav.farskapsportal.persistence.entity.Signeringsinformasjon;
import no.nav.farskapsportal.util.Mapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

@DisplayName("FarskapserklaeringService")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = FarskapsportalApplicationLocal.class)
@ActiveProfiles(PROFILE_TEST)
public class FarskapsportalServiceTest {

  private static final ForelderDto MOR = henteForelder(Forelderrolle.MOR);
  private static final ForelderDto FAR = henteForelder(Forelderrolle.FAR);
  private static final BarnDto BARN = henteBarnUtenFnr(5);

  @MockBean
  PdfGeneratorConsumer pdfGeneratorConsumer;
  @MockBean
  DifiESignaturConsumer difiESignaturConsumer;
  @MockBean
  PersonopplysningService personopplysningService;
  @MockBean
  SkattConsumer skattConsumer;
  @Autowired
  private PersistenceService persistenceService;
  @Autowired
  private FarskapserklaeringDao farskapserklaeringDao;
  @Autowired
  private ForelderDao forelderDao;
  @Autowired
  private StatusKontrollereFarDao statusKontrollereFarDao;
  @Autowired
  private FarskapsportalService farskapsportalService;
  @Autowired
  private FarskapsportalEgenskaper farskapsportalEgenskaper;
  @Autowired
  private Mapper mapper;

  @Nested
  @DisplayName("Teste henteBrukerinformasjon")
  class HenteBrukerinformasjon {

    @Test
    @DisplayName("Mor skal se liste over nyfødte uten far")
    void morSkalSeSinePaabegynteOgFarsVentedeFarskapserklaeringerOgListeOverNyfoedteUtenFar() {

      // given
      farskapserklaeringDao.deleteAll();

      var foedselsdatoSpedbarn = LocalDate.now().minusMonths(2).minusDays(21);
      var spedbarnUtenFar = BarnDto.builder().foedselsnummer(foedselsdatoSpedbarn.format(DateTimeFormatter.ofPattern("ddMMyy")) + "10100").build();

      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now().minusMinutes(3));

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);

      when(personopplysningService.henteNyligFoedteBarnUtenRegistrertFar(MOR.getFoedselsnummer()))
          .thenReturn(Set.of(spedbarnUtenFar.getFoedselsnummer()));

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(MOR.getFoedselsnummer());

      assertAll(() -> assertEquals(1, brukerinformasjon.getFnrNyligFoedteBarnUtenRegistrertFar().size()), () -> assertEquals(1,
          brukerinformasjon.getAvventerSigneringMotpart().stream().filter(fe -> fe.getPaaloggetBrukersRolle().equals(Rolle.MOR))
              .collect(Collectors.toSet()).size()));

    }

    @Test
    @DisplayName("Mor skal se sine påbegynte farskapserklæringer")
    void morSkalSeSinePaabegynteFarskapserklaeringer() {

      // given
      farskapserklaeringDao.deleteAll();

      var farskapserklaeringSomManglerMorsSignatur = henteFarskapserklaering(MOR, FAR, BARN);

      assertAll(() -> assertNull(farskapserklaeringSomManglerMorsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomManglerMorsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomManglerMorsSignatur);

      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(MOR.getFoedselsnummer());

      // then
      assertAll(() -> assertEquals(1,
          brukerinformasjon.getAvventerSigneringBruker().stream().filter(fe -> fe.getPaaloggetBrukersRolle().equals(Rolle.MOR))
              .collect(Collectors.toSet()).size()), () -> assertEquals(0, brukerinformasjon.getFnrNyligFoedteBarnUtenRegistrertFar().size()),
          () -> assertEquals(0, brukerinformasjon.getAvventerSigneringMotpart().size()));
    }

    @Test
    @DisplayName("Mor skal se farskapserklæringer som venter på far")
    void morSkalSeFarskapserklaeringerSomVenterPaaFar() {

      // given
      farskapserklaeringDao.deleteAll();
      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(MOR.getFoedselsnummer());

      assertEquals(1, brukerinformasjon.getAvventerSigneringMotpart().stream().filter(fe -> fe.getPaaloggetBrukersRolle().equals(Rolle.MOR))
          .collect(Collectors.toSet()).size());

    }

    @Test
    @DisplayName("Skal kaste ValideringException dersom mor er gift")
    void skalKasteValideringExceptionDersomMorErGift() {

      // given
      farskapserklaeringDao.deleteAll();

      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.GIFT).build());
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.henteBrukerinformasjon(MOR.getFoedselsnummer()));
    }

    @Test
    @DisplayName("Skal kaste ValideringException dersom mor har ukjent sivilstand")
    void skalKasteValideringExceptionDersomMorHarUkjentSivilstand() {

      // given
      farskapserklaeringDao.deleteAll();

      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer()))
          .thenReturn(SivilstandDto.builder().type(Sivilstandtype.UOPPGITT).build());
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);

      // when
      assertThrows(ValideringException.class, () -> farskapsportalService.henteBrukerinformasjon(MOR.getFoedselsnummer()));
    }

    @Test
    @DisplayName("Skal kaste ValideringException dersom mor er registrert partner")
    void skalKasteValideringExceptiondersomMorErRegistrertPartner() {

      // given
      farskapserklaeringDao.deleteAll();

      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer()))
          .thenReturn(SivilstandDto.builder().type(Sivilstandtype.REGISTRERT_PARTNER).build());
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.henteBrukerinformasjon(MOR.getFoedselsnummer()));
    }

    @Test
    @DisplayName("Far skal se sine ventende farskapserklæringer")
    void farSkalSeSineVentendeFarskapserklaeringer() {

      // given
      farskapserklaeringDao.deleteAll();
      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);
      when(personopplysningService.henteSivilstand(FAR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(FAR.getFoedselsnummer());

      // then
      assertEquals(1, brukerinformasjon.getAvventerSigneringBruker().stream().filter(fe -> fe.getPaaloggetBrukersRolle().equals(Rolle.FAR))
          .collect(Collectors.toSet()).size());
    }

    @Test
    @DisplayName("Far skal ikke se farskapserklæringer som mor ikke har signert")
    void farSkalIkkeSeFarskapserklaeringerSomMorIkkeHarSignert() {

      // given
      farskapserklaeringDao.deleteAll();
      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);

      assertAll(() -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);
      when(personopplysningService.henteSivilstand(FAR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(FAR.getFoedselsnummer());

      // then
      assertAll(() -> assertEquals(0, brukerinformasjon.getAvventerSigneringBruker().size()),
          () -> assertEquals(0, brukerinformasjon.getAvventerSigneringMotpart().size()),
          () -> assertEquals(0, brukerinformasjon.getFnrNyligFoedteBarnUtenRegistrertFar().size()));
    }
  }

  @Nested
  @DisplayName("Teste oppretteFarskapserklaering")
  class OppretteFarskapserklaering {

    @SneakyThrows
    @Test
    @DisplayName("Skal opprette farskapserklæring for barn med termindato")
    void skalOppretteFarskapserklaeringForBarnMedTermindato() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var barn = henteBarnUtenFnr(4);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = Dokument.builder().dokumentnavn("Farskapserklæering.pdf")
          .dokumentinnhold(Dokumentinnhold.builder().innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl("https://esignering.no/redirect-mor").build()).build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);

      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);

      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when
      var respons = farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder()
              .barn(barn)
              .opplysningerOmFar(opplysningerOmFar)
              .morBorSammenMedFar(true)
              .build());

      // then
      var opprettetFarskapserklaering = persistenceService.henteFarskapserklaeringerForForelder(MOR.getFoedselsnummer());
      assertAll(
          () -> assertThat(opprettetFarskapserklaering.size()).isEqualTo(1),
          () -> assertThat(opprettetFarskapserklaering.stream().findAny().get().getMorBorSammenMedFar()).isTrue(),
          () -> assertThat(respons.getRedirectUrlForSigneringMor()).isEqualTo(pdf.getSigneringsinformasjonMor().getRedirectUrl())
      );
    }

    @Test
    @DisplayName("Skal opprette farskapserklæring for nyfødt")
    void skalOppretteFarskapserklaeringForNyfoedt() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var foedselsdatoBarn = LocalDate.now().minusMonths(farskapsportalEgenskaper.getMaksAntallMaanederEtterFoedsel()).plusDays(1);
      var barnFoedtInnenforGyldigIntervall = henteBarnMedFnr(foedselsdatoBarn);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = Dokument.builder()
          .dokumentnavn("Farskapserklæering.pdf")
          .dokumentinnhold(Dokumentinnhold.builder()
              .innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl("https://esignering.no/redirect-mor").build())
          .build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteNyligFoedteBarnUtenRegistrertFar(MOR.getFoedselsnummer()))
          .thenReturn(Set.of(barnFoedtInnenforGyldigIntervall.getFoedselsnummer()));
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);

      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);

      when(personopplysningService.henteFoedselsdato(barnFoedtInnenforGyldigIntervall.getFoedselsnummer())).thenReturn(foedselsdatoBarn);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when
      var respons = farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder().barn(barnFoedtInnenforGyldigIntervall).opplysningerOmFar(opplysningerOmFar).build());

      // then
      assertEquals(pdf.getSigneringsinformasjonMor().getRedirectUrl(), respons.getRedirectUrlForSigneringMor());
    }

    @Test
    @DisplayName("Mor skal kunne opprette farskapserklæring for nyfødt barn selv om hun har en pågående farskapserklæring for ufødt")
    void morSkalKunneOppretteFarskapserklaeringForNyfoedtSelvOmHunHarEnAapenErklaeringForUfoedt() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var foedselsdatoBarn = LocalDate.now().minusMonths(farskapsportalEgenskaper.getMaksAntallMaanederEtterFoedsel()).plusDays(1);
      var barnFoedtInnenforGyldigIntervall = henteBarnMedFnr(foedselsdatoBarn);
      var eksisterendeFarskapserklaeringUfoedtBarnVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder()
          .foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();
      var redirectUrlMor = "https://esignering.no/redirect-mor";
      var pdf = Dokument.builder().dokumentnavn("Farskapserklæering.pdf")
          .dokumentinnhold(Dokumentinnhold.builder().innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl(redirectUrlMor).build()).build();

      eksisterendeFarskapserklaeringUfoedtBarnVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());
      assertAll(() -> assertNotNull(eksisterendeFarskapserklaeringUfoedtBarnVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(eksisterendeFarskapserklaeringUfoedtBarnVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(eksisterendeFarskapserklaeringUfoedtBarnVenterPaaFarsSignatur);

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteNyligFoedteBarnUtenRegistrertFar(MOR.getFoedselsnummer()))
          .thenReturn(Set.of(barnFoedtInnenforGyldigIntervall.getFoedselsnummer()));

      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);

      when(personopplysningService.henteFoedselsdato(barnFoedtInnenforGyldigIntervall.getFoedselsnummer())).thenReturn(foedselsdatoBarn);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when
      var respons = farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder()
              .barn(barnFoedtInnenforGyldigIntervall)
              .opplysningerOmFar(opplysningerOmFar)
              .morBorSammenMedFar(true)
              .build());

      // then
      assertThat(redirectUrlMor).isEqualTo(respons.getRedirectUrlForSigneringMor());
    }

    @Test
    @DisplayName("Mor skal ikke kunne opprette farskapserklæring for ufødt barn dersom hun har en pågående farskapserklæring")
    void morSkalIkkeKunneOppretteFarskapserklaeringForUfoedtBarnDersomHunHarEnPaagaaendeFarskapserklaering() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var ufoedtBarn = henteBarnUtenFnr(13);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);

      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder().barn(ufoedtBarn).opplysningerOmFar(opplysningerOmFar).build()));
    }

    @Test
    @DisplayName("Skal kaste ForskjelligeFedreException dersom mor har åpen erklæring med annen far for nyfødte barn")
    void skalKasteForskjelligeFedreExceptionDersomMorHarAapenErklaeringMedAnnenFarForNyfoedteBarn() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var enAnnenFar = ForelderDto.builder().fornavn("Svampe").etternavn("Bob")
          .foedselsnummer(LocalDate.now().minusYears(35).format(DateTimeFormatter.ofPattern("ddMMyy")) + "01011").build();
      var foedselsdatoNyfoedte = LocalDate.now().minusMonths(1);
      var nyfoedtBarn1 = henteBarnMedFnr(foedselsdatoNyfoedte);
      var nyfoedtBarn2 = BarnDto.builder().foedselsnummer(foedselsdatoNyfoedte.format(DateTimeFormatter.ofPattern("ddMMyy")) + "11111").build();
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();
      var farskapserklaeringSomVenterPaaEnAnnenFarsSignatur = henteFarskapserklaering(MOR, enAnnenFar, nyfoedtBarn1);
      var pdf = Dokument.builder().dokumentnavn("Farskapserklæering.pdf")
          .dokumentinnhold(Dokumentinnhold.builder().innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl("https://esignering.no/redirect-mor").build()).build();

      farskapserklaeringSomVenterPaaEnAnnenFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaEnAnnenFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaEnAnnenFarsSignatur.getDokument().getSignertAvFar()));

      persistenceService.lagreNyFarskapserklaering(farskapserklaeringSomVenterPaaEnAnnenFarsSignatur);

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteNyligFoedteBarnUtenRegistrertFar(MOR.getFoedselsnummer()))
          .thenReturn(Set.of(nyfoedtBarn2.getFoedselsnummer()));

      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);

      when(personopplysningService.henteFoedselsdato(nyfoedtBarn2.getFoedselsnummer())).thenReturn(foedselsdatoNyfoedte);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder().barn(nyfoedtBarn2).opplysningerOmFar(opplysningerOmFar).build()));
    }

    @Test
    @DisplayName("Skal kaste IllegalArgumentException dersom mor og far er samme person")
    void skalKasteIllegalArgumentExceptionDersomMorOgFarErSammePerson() {

      // given
      var barn = henteBarnUtenFnr(4);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(MOR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = Dokument.builder().dokumentnavn("Farskapserklæering.pdf")
          .dokumentinnhold(Dokumentinnhold.builder().innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl("https://esignering.no/redirect-mor").build()).build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);

      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when, then
      assertThrows(IllegalArgumentException.class, () -> farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder().barn(barn).opplysningerOmFar(opplysningerOmFar).build()));
    }

    @Test
    @DisplayName("Skal kaste ValideringException dersom termindato er ugyldig")
    void skalKasteValideringExceptionDersomTermindatoErUgyldig() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      //given
      var barnMedTermindatoForLangtFremITid = henteBarnUtenFnr(farskapsportalEgenskaper.getMaksAntallUkerTilTermindato() + 2);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = Dokument.builder().dokumentnavn("Farskapserklæering.pdf")
          .dokumentinnhold(Dokumentinnhold.builder().innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl("https://esignering.no/redirect-mor").build()).build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);

      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder().barn(barnMedTermindatoForLangtFremITid).opplysningerOmFar(opplysningerOmFar).build()));
    }

    @Test
    @DisplayName("Skal kaste ManglerRelasjonException dersom barn oppgitt med fødselsnummer mangler relasjon til mor")
    void skalKasteManglerRelasjonExceptionDersomBarnOppgittMedFoedselsnummerManglerRelasjonTilMor() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var fnrSpedbarnUtenFar = LocalDate.now().minusMonths(2).minusDays(-5).format(DateTimeFormatter.ofPattern("ddMMyy")) + "13333";
      var barnUtenRelasjonTilMor = BarnDto.builder()
          .foedselsnummer(LocalDate.now().minusMonths(2).minusDays(21).format(DateTimeFormatter.ofPattern("ddMMyy")) + "10100").build();
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = Dokument.builder().dokumentnavn("Farskapserklæering.pdf")
          .dokumentinnhold(Dokumentinnhold.builder().innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl("https://esignering.no/redirect-mor").build()).build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.henteNyligFoedteBarnUtenRegistrertFar(MOR.getFoedselsnummer())).thenReturn(new HashSet<>());

      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      when(personopplysningService.henteNyligFoedteBarnUtenRegistrertFar(MOR.getFoedselsnummer())).thenReturn(Set.of(fnrSpedbarnUtenFar));

      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when, then
      assertThrows(ManglerRelasjonException.class, () -> farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder().barn(barnUtenRelasjonTilMor).opplysningerOmFar(opplysningerOmFar).build()));

    }

    @Test
    @DisplayName("Skal kaste MorHarIngenNyfoedteUtenFar exception dersom mor ikke er registrert med nyfødte ban uten far")
    void skalKasteMorHarIngenNyfoedteUtenFarException() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var nyfoedt = henteNyligFoedtBarn();
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = Dokument.builder()
          .dokumentnavn("Farskapserklæering.pdf")
          .dokumentinnhold(Dokumentinnhold.builder()
              .innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl("https://esignering.no/redirect-mor").build()).build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.henteNyligFoedteBarnUtenRegistrertFar(MOR.getFoedselsnummer())).thenReturn(new HashSet<>());

      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);

      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when, then
      assertThrows(MorHarIngenNyfoedteUtenFarException.class, () -> farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder().barn(nyfoedt).opplysningerOmFar(opplysningerOmFar).build()));
    }

    @Test
    @DisplayName("Skal kaste NyfoedtErForGammelException dersom nyfødt er for gammel")
    void skalKasteNyfoedtErForGammelExceptionDersomNyfoedtErForGammel() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var foedselsdatoNyfoedt = LocalDate.now().minusMonths(farskapsportalEgenskaper.getMaksAntallMaanederEtterFoedsel());
      var nyfoedt = henteBarnMedFnr(foedselsdatoNyfoedt);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = Dokument.builder().dokumentnavn("Farskapserklæering.pdf")
          .dokumentinnhold(Dokumentinnhold.builder().innhold("Jeg erklærer med dette farskap til barnet..".getBytes()).build())
          .signeringsinformasjonMor(Signeringsinformasjon.builder().redirectUrl("https://esignering.no/redirect-mor").build()).build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteNyligFoedteBarnUtenRegistrertFar(MOR.getFoedselsnummer())).thenReturn(Set.of(nyfoedt.getFoedselsnummer()));

      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);

      when(personopplysningService.henteFoedselsdato(nyfoedt.getFoedselsnummer())).thenReturn(foedselsdatoNyfoedt);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);

      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when, then
      assertThrows(NyfoedtErForGammelException.class, () -> farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskapserklaeringRequest.builder().barn(nyfoedt).opplysningerOmFar(opplysningerOmFar).build()));
    }
  }

  @Nested
  @DisplayName("Teste oppdatereStatus")
  class OppdatereStatus {

    @Test
    void skalOppdatereSigneringsinformasjonForMorEtterRedirectDersomStatusQueryTokenErGyldig() {

      // given
      farskapserklaeringDao.deleteAll();

      var statuslenke = lageUrl("/status");
      var farskapserklaering = henteFarskapserklaering(MOR, FAR, BARN);
      var padesMor = lageUrl("/padesMor");
      var farskapserklaeringDokumentinnhold = "Jeg erklærer herved farskap til dette barnet".getBytes(StandardCharsets.UTF_8);
      var xadesXml = "<xades><signerer>12345678912</signerer></xades>".getBytes(StandardCharsets.UTF_8);

      assertNull(farskapserklaering.getDokument().getSignertAvMor());

      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(farskapserklaering);
      lagretFarskapserklaering.getDokument().setDokumentStatusUrl(statuslenke.toString());
      farskapserklaeringDao.save(lagretFarskapserklaering);

      when(personopplysningService.henteFoedselsdato(MOR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_MOR);
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      when(personopplysningService.henteGjeldendeKjoenn(MOR.getFoedselsnummer())).thenReturn(KjoennDto.builder().kjoenn(KjoennType.KVINNE).build());

      when(difiESignaturConsumer.henteStatus(any(), any())).thenReturn(
          DokumentStatusDto.builder()
              .bekreftelseslenke(lageUrl("/confirmation"))
              .statuslenke(statuslenke)
              .erSigneringsjobbenFerdig(true)
              .padeslenke(padesMor).signaturer(List.of(
              SignaturDto.builder().signatureier(MOR.getFoedselsnummer()).harSignert(true).tidspunktForStatus(LocalDateTime.now().minusSeconds(3))
                  .build())).build());

      when(difiESignaturConsumer.henteSignertDokument(any())).thenReturn(farskapserklaeringDokumentinnhold);
      when(difiESignaturConsumer.henteXadesXml(any())).thenReturn(xadesXml);

      // when
      farskapsportalService.oppdatereStatus(MOR.getFoedselsnummer(), "etGyldigStatusQueryToken");

      var oppdatertFarskapserklaering = farskapserklaeringDao.findById(lagretFarskapserklaering.getId());

      // then
      assertAll(
          () -> assertThat(oppdatertFarskapserklaering).isPresent(),
          () -> assertNotNull(oppdatertFarskapserklaering.get().getDokument().getSigneringsinformasjonMor().getSigneringstidspunkt()),
          () -> assertArrayEquals(farskapserklaeringDokumentinnhold,
              oppdatertFarskapserklaering.get().getDokument().getDokumentinnhold().getInnhold()),
          () -> assertArrayEquals(xadesXml, oppdatertFarskapserklaering.get().getDokument().getSigneringsinformasjonMor().getXadesXml())
      );
    }

    @Test
    void skalOppdatereSigneringsinformasjonForFarEtterRedirectDersomStatusQueryTokenErGyldig() {

      // given
      farskapserklaeringDao.deleteAll();

      var statuslenke = lageUrl("/status");
      var farskapserklaering = henteFarskapserklaering(MOR, FAR, BARN);
      var padesFar = lageUrl("/padesFar");
      farskapserklaering.getDokument().setSignertAvMor(LocalDateTime.now().minusMinutes(3));
      var farskapserklaeringDokumentinnhold = "Jeg erklærer herved farskap til dette barnet".getBytes(StandardCharsets.UTF_8);
      var xadesXml = "<xades><signerer>12345678912</signerer></xades>".getBytes(StandardCharsets.UTF_8);

      assertNull(farskapserklaering.getDokument().getSignertAvFar());

      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(farskapserklaering);
      lagretFarskapserklaering.getDokument().setDokumentStatusUrl(statuslenke.toString());
      farskapserklaeringDao.save(lagretFarskapserklaering);

      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);
      when(personopplysningService.henteGjeldendeKjoenn(FAR.getFoedselsnummer())).thenReturn(KjoennDto.builder().kjoenn(KjoennType.MANN).build());

      when(difiESignaturConsumer.henteStatus(any(), any())).thenReturn(
          DokumentStatusDto.builder()
              .bekreftelseslenke(lageUrl("/confirmation"))
              .statuslenke(statuslenke)
              .erSigneringsjobbenFerdig(true)
              .padeslenke(padesFar).signaturer(List.of(
              SignaturDto.builder().signatureier(FAR.getFoedselsnummer()).harSignert(true).tidspunktForStatus(LocalDateTime.now().minusSeconds(3))
                  .build())).build());

      when(difiESignaturConsumer.henteSignertDokument(any())).thenReturn(farskapserklaeringDokumentinnhold);
      when(difiESignaturConsumer.henteXadesXml(any())).thenReturn(xadesXml);

      // when
      farskapsportalService.oppdatereStatus(FAR.getFoedselsnummer(), "etGyldigStatusQueryToken");

      var oppdatertFarskapserklaering = farskapserklaeringDao.findById(lagretFarskapserklaering.getId());

      // then
      assertAll(
          () -> assertThat(oppdatertFarskapserklaering).isPresent(),
          () -> assertNotNull(oppdatertFarskapserklaering.get().getDokument().getSigneringsinformasjonFar().getSigneringstidspunkt()),
          () -> assertArrayEquals(farskapserklaeringDokumentinnhold,
              oppdatertFarskapserklaering.get().getDokument().getDokumentinnhold().getInnhold()),
          () -> assertArrayEquals(xadesXml, oppdatertFarskapserklaering.get().getDokument().getSigneringsinformasjonFar().getXadesXml())
      );
    }
  }

  @Nested
  @DisplayName("HenteNyRedirectUrl")
  class HenteNyRedirectUrl {

    @Test
    void skalHenteNyRedirectUrlForMorDersomMorsUndertegnerurlErRiktig() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var farskapserklaering = mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN));
      var undertegnerUrlMor = lageUrl("/signer-url-mor");
      farskapserklaering.getDokument().getSigneringsinformasjonMor().setUndertegnerUrl(undertegnerUrlMor.toString());

      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(farskapserklaering);

      var fnrPaaloggetPerson = MOR.getFoedselsnummer();
      var idFarskapserklaering = lagretFarskapserklaering.getId();
      var nyRedirectUrl = lageUrl("/ny-redirect");

      when(difiESignaturConsumer.henteNyRedirectUrl(undertegnerUrlMor)).thenReturn(nyRedirectUrl);

      // when
      var returnertRedirectUrl = farskapsportalService.henteNyRedirectUrl(fnrPaaloggetPerson, idFarskapserklaering);
      var oppdatertFarskapserklaering = persistenceService.henteFarskapserklaeringForId(lagretFarskapserklaering.getId());

      // then
      assertAll(() -> assertThat(nyRedirectUrl).isEqualTo(returnertRedirectUrl), () -> assertThat(nyRedirectUrl.toString())
          .isEqualTo(oppdatertFarskapserklaering.getDokument().getSigneringsinformasjonMor().getRedirectUrl()));
    }

    @Test
    void skalOppdatereLagretFarskapserklaeringMedNyRedirectUrlForFar() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var farskapserklaering = mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN));
      var undertegnerUrlFar = lageUrl("/signer-url-far");
      farskapserklaering.getDokument().getSigneringsinformasjonFar().setUndertegnerUrl(undertegnerUrlFar.toString());
      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(farskapserklaering);

      var fnrPaaloggetPerson = FAR.getFoedselsnummer();
      var nyRedirectUrlFar = lageUrl("/ny-redirect-far");
      when(difiESignaturConsumer.henteNyRedirectUrl(undertegnerUrlFar)).thenReturn(nyRedirectUrlFar);

      // when
      var returnertRedirectUrl = farskapsportalService.henteNyRedirectUrl(fnrPaaloggetPerson, lagretFarskapserklaering.getId());
      var oppdatertFarskapserklaering = persistenceService.henteFarskapserklaeringForId(lagretFarskapserklaering.getId());

      // then
      assertAll(() -> assertThat(nyRedirectUrlFar).isEqualTo(returnertRedirectUrl), () -> assertThat(nyRedirectUrlFar.toString())
          .isEqualTo(oppdatertFarskapserklaering.getDokument().getSigneringsinformasjonFar().getRedirectUrl()));
    }

    @Test
    void skalKasteRessursIkkeFunnetExceptionVedHentingAvNyRedirectUrlDersomFarskapserklaeringIkkeFinnes() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var fnrPaaloggetPerson = MOR.getFoedselsnummer();
      var idFarskapserklaeringSomIkkeEksisterer = 0;

      // when, then
      assertThrows(RessursIkkeFunnetException.class,
          () -> farskapsportalService.henteNyRedirectUrl(fnrPaaloggetPerson, idFarskapserklaeringSomIkkeEksisterer));
    }

    @Test
    void skalKasteValideringExceptionDersomPaaloggetPersonIkkeErPartIFarskapserklaering() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var farskapserklaering = mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN));
      var undertegnerUrlMor = lageUrl("/signer-url-mor");
      farskapserklaering.getDokument().getSigneringsinformasjonMor().setUndertegnerUrl(undertegnerUrlMor.toString());

      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(farskapserklaering);

      var fnrPaaloggetPerson = "00000000000";

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.henteNyRedirectUrl(fnrPaaloggetPerson, lagretFarskapserklaering.getId()));
    }
  }

  @Nested
  @DisplayName("Teste kontrollereFar")
  class KontrollereFar {

    @Test
    @DisplayName("Skal ikke kaste exception dersom fars navn er oppgitt riktig")
    void skalIkkeKasteExceptionDersomFarsNavnErOppgittRiktig() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);
      doNothing().when(personopplysningService).navnekontroll(opplysningerOmFar.getNavn(), registrertNavnFar);

      // when, then
      assertDoesNotThrow(() -> farskapsportalService.kontrollereFar(MOR.getFoedselsnummer(), opplysningerOmFar));

      // rydde testdata
      statusKontrollereFarDao.deleteAll();
    }

    @Test
    void skalIkkeKasteExceptionDersomFarHarForelderrolleMorEllerFar() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR_ELLER_FAR);
      doNothing().when(personopplysningService).navnekontroll(opplysningerOmFar.getNavn(), registrertNavnFar);

      // when, then
      assertDoesNotThrow(() -> farskapsportalService.kontrollereFar(MOR.getFoedselsnummer(), opplysningerOmFar));

      // rydde testdata
      statusKontrollereFarDao.deleteAll();
    }

    @Test
    void skalKasteValideringExceptionDersomFarHarForelderrolleMor() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);
      doNothing().when(personopplysningService).navnekontroll(opplysningerOmFar.getNavn(), registrertNavnFar);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.kontrollereFar(MOR.getFoedselsnummer(), opplysningerOmFar));

      // rydde testdata
      statusKontrollereFarDao.deleteAll();
    }

    @Test
    void skalKasteValideringExceptionDersomFarErUmyndig() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(LocalDate.now().minusYears(17));
      doThrow(ValideringException.class).when(personopplysningService).erMyndig(FAR.getFoedselsnummer());
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);
      doNothing().when(personopplysningService).navnekontroll(opplysningerOmFar.getNavn(), registrertNavnFar);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.kontrollereFar(MOR.getFoedselsnummer(), opplysningerOmFar));

      // rydde testdata
      statusKontrollereFarDao.deleteAll();
    }

    @Test
    @DisplayName("Skal kaste FeilNavnOppgittException dersom fars navn er oppgitt feil")
    void skalKasteFeilNavnOppgittExceptionDersomFarsNavnErOppgittFeil() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer()).navn("Borat Sagidyev").build();
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);
      doThrow(FeilNavnOppgittException.class).when(personopplysningService).navnekontroll(opplysningerOmFar.getNavn(), registrertNavnFar);

      // when, then
      assertThrows(FeilNavnOppgittException.class, () -> farskapsportalService.kontrollereFar(MOR.getFoedselsnummer(), opplysningerOmFar));

      // rydde testdata
      statusKontrollereFarDao.deleteAll();
    }

    @Test
    @DisplayName("Skal kaste exception dersom antall forsøk er brukt opp")
    void skalKasteExceptionDersomAntallForsoekErBruktOpp() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer()).navn("Borat Sagidyev").build();
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteFoedselsdato(FAR.getFoedselsnummer())).thenReturn(FOEDSELSDATO_FAR);
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);
      doThrow(FeilNavnOppgittException.class).when(personopplysningService).navnekontroll(opplysningerOmFar.getNavn(), registrertNavnFar);

      // when
      // Bruker opp antall mulige forsøk på å finne frem til riktig kombinasjon av fnr og navn
      for (int i = 0; i < 5; i++) {
        assertThrows(FeilNavnOppgittException.class, () -> farskapsportalService.kontrollereFar(MOR.getFoedselsnummer(), opplysningerOmFar));
      }

      // then
      // Sjette forsøk gir ValideringsException ettersom antall mulige forsøk er brukt opp
      assertThrows(ValideringException.class, () -> farskapsportalService.kontrollereFar(MOR.getFoedselsnummer(), opplysningerOmFar));

      // rydde testdata
      statusKontrollereFarDao.deleteAll();

    }
  }

  @Nested
  @DisplayName("Teste validereMor")
  class ValidereMor {

    @Test
    void myndigPersonMedFoedekjoennKvinneKanOpptreSomMor() {

      // given
      doNothing().when(personopplysningService).erMyndig(MOR.getFoedselsnummer());
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);

      // when, then
      assertDoesNotThrow(() -> farskapsportalService.validereMor(MOR.getFoedselsnummer()));

    }

    @Test
    void myndigGiftPersonMedFoedekjoennKvinneKanIkkeOpptreSomMor() {

      // given
      doNothing().when(personopplysningService).erMyndig(MOR.getFoedselsnummer());
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.GIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.validereMor(MOR.getFoedselsnummer()));

    }

    @Test
    void umyndigPersonMedFoedekjoennKvinneKanIkkeOpptreSomMor() {

      // given
      doThrow(ValideringException.class).when(personopplysningService).erMyndig(MOR.getFoedselsnummer());
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.GIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.validereMor(MOR.getFoedselsnummer()));
    }

    @Test
    void myndigMannMedFoedekjoennKvinneKanOpptreSomMor() {

      // given
      doNothing().when(personopplysningService).erMyndig(MOR.getFoedselsnummer());
      when(personopplysningService.henteSivilstand(MOR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(Forelderrolle.MOR_ELLER_FAR);

      // when, then
      assertDoesNotThrow(() -> farskapsportalService.validereMor(MOR.getFoedselsnummer()));
    }

    @Test
    void myndigMannMedFoedekjoennMannKanIkkeOpptreSomMor() {

      // given
      doNothing().when(personopplysningService).erMyndig(FAR.getFoedselsnummer());
      when(personopplysningService.henteSivilstand(FAR.getFoedselsnummer())).thenReturn(SivilstandDto.builder().type(Sivilstandtype.UGIFT).build());
      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(Forelderrolle.FAR);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.validereMor(FAR.getFoedselsnummer()));
    }

  }

  @Nested
  class OppdaterFarskapserklaering {

    @Test
    void skalOppdatereBorSammeninformasjonForMorDersomPersonErMorIFarskapserklaeringen() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var farskapserklaering = mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN));
      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(farskapserklaering);

      var fnrPaaloggetPerson = MOR.getFoedselsnummer();
      var request = OppdatereFarskapserklaeringRequest.builder().idFarskapserklaering(lagretFarskapserklaering.getId()).borSammen(true).build();

      // when
      var respons = farskapsportalService.oppdatereFarskapserklaering(fnrPaaloggetPerson, request);

      // then
      assertAll(() -> assertThat(respons.getOppdatertFarskapserklaeringDto().getMorBorSammenMedFar()).isTrue(),
          () -> assertThat(respons.getOppdatertFarskapserklaeringDto().getFarBorSammenMedMor()).isNull());
    }

    @Test
    void skalOppdatereBorSammeninformasjonForFaDersomPersonErFarIFarskapserklaeringen() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var farskapserklaering = mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN));
      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(farskapserklaering);

      var fnrPaaloggetPerson = FAR.getFoedselsnummer();
      var request = OppdatereFarskapserklaeringRequest.builder().idFarskapserklaering(lagretFarskapserklaering.getId()).borSammen(true).build();

      // when
      var respons = farskapsportalService.oppdatereFarskapserklaering(fnrPaaloggetPerson, request);

      // then
      assertAll(() -> assertThat(respons.getOppdatertFarskapserklaeringDto().getFarBorSammenMedMor()).isTrue(),
          () -> assertThat(respons.getOppdatertFarskapserklaeringDto().getMorBorSammenMedFar()).isNull());
    }

    @Test
    void skalKasteValideringExceptionDersomPersonIkkeErPartIFarskapserklaeringeg() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var farskapserklaering = mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN));
      var lagretFarskapserklaering = persistenceService.lagreNyFarskapserklaering(farskapserklaering);

      var fnrPaaloggetPerson = "12345678910";
      var request = OppdatereFarskapserklaeringRequest.builder().idFarskapserklaering(lagretFarskapserklaering.getId()).borSammen(true).build();

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService.oppdatereFarskapserklaering(fnrPaaloggetPerson, request));
    }
  }

  @Nested
  class HenteDokumentinnhold {

    @Test
    void skalHenteDokumentinnholdForFarMedVentendeErklaering() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();
      forelderDao.deleteAll();

      // given
      var farskapserklaering = farskapserklaeringDao.save(mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN)));
      farskapserklaering.getDokument().getSigneringsinformasjonMor().setSigneringstidspunkt(LocalDateTime.now());
      farskapserklaering.getDokument().setDokumentinnhold(
          Dokumentinnhold.builder().innhold("Jeg erklærer herved farskap til dette barnet".getBytes(StandardCharsets.UTF_8)).build());
      farskapserklaeringDao.save(farskapserklaering);

      // when
      var dokumentinnhold = farskapsportalService.henteDokumentinnhold(FAR.getFoedselsnummer(), farskapserklaering.getId());

      // then
      assertArrayEquals(farskapserklaering.getDokument().getDokumentinnhold().getInnhold(), dokumentinnhold);
    }

    @Test
    void skalKasteExceptionDersomPersonIkkeErPartIErklaeringen() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();
      forelderDao.deleteAll();

      // given
      var farskapserklaering = farskapserklaeringDao.save(mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN)));
      farskapserklaering.getDokument().getSigneringsinformasjonMor().setSigneringstidspunkt(LocalDateTime.now());
      farskapserklaeringDao.save(farskapserklaering);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService
          .henteDokumentinnhold(FOEDSELSDATO_FAR.format(DateTimeFormatter.ofPattern("ddMMyy")) + "35351", farskapserklaering.getId()));
    }

    @Test
    void skalKasteRessursIkkeFunnetExceptionForFarDersomErklaeringIkkeFinnes() {

      // given
      var idFarskapserklaeringSomIkkeFinnes = 123;

      // when, then
      assertThrows(RessursIkkeFunnetException.class, () -> farskapsportalService
          .henteDokumentinnhold(FAR.getFoedselsnummer(), idFarskapserklaeringSomIkkeFinnes));

    }

    @Test
    void skalKasteExceptionForFarHvisMorIkkeHarSignert() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();
      forelderDao.deleteAll();

      // given
      var farskapserklaering = farskapserklaeringDao.save(mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN)));
      farskapserklaeringDao.save(farskapserklaering);

      // when, then
      assertThrows(ValideringException.class, () -> farskapsportalService
          .henteDokumentinnhold(FAR.getFoedselsnummer(), farskapserklaering.getId()));

    }

    @Test
    void skalHenteDokumentForMorMedAktivErklaering() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();
      forelderDao.deleteAll();

      // given
      var farskapserklaering = farskapserklaeringDao.save(mapper.toEntity(henteFarskapserklaering(MOR, FAR, BARN)));
      farskapserklaering.getDokument().setDokumentinnhold(
          Dokumentinnhold.builder().innhold("Jeg erklærer herved farskap til dette barnet".getBytes(StandardCharsets.UTF_8)).build());
      farskapserklaeringDao.save(farskapserklaering);

      // when
      var dokumentinnhold = farskapsportalService.henteDokumentinnhold(MOR.getFoedselsnummer(), farskapserklaering.getId());

      // then
      assertArrayEquals(farskapserklaering.getDokument().getDokumentinnhold().getInnhold(), dokumentinnhold);
    }

    @Test
    void skalKasteRessursIkkeFunnetExceptionForMorUtenAktiveErklaeringer() {

      // rydde testdata
      farskapserklaeringDao.deleteAll();

      // given
      var idFarskapserklaeringSomIkkeFinnes = 1525;

      // when, then
      assertThrows(RessursIkkeFunnetException.class, () -> farskapsportalService
          .henteDokumentinnhold(FAR.getFoedselsnummer(), idFarskapserklaeringSomIkkeFinnes));
    }
  }
}

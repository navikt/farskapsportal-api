package no.nav.farskapsportal.service;

import static no.nav.farskapsportal.FarskapsportalApplicationLocal.PROFILE_TEST;
import static no.nav.farskapsportal.TestUtils.henteBarn;
import static no.nav.farskapsportal.TestUtils.henteFarskapserklaering;
import static no.nav.farskapsportal.TestUtils.henteForelder;
import static no.nav.farskapsportal.TestUtils.lageUrl;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import lombok.SneakyThrows;
import no.nav.farskapsportal.FarskapsportalApplicationLocal;
import no.nav.farskapsportal.api.Forelderrolle;
import no.nav.farskapsportal.api.KontrollerePersonopplysningerRequest;
import no.nav.farskapsportal.api.OppretteFarskaperklaeringRequest;
import no.nav.farskapsportal.config.FarskapsportalEgenskaper;
import no.nav.farskapsportal.consumer.esignering.DifiESignaturConsumer;
import no.nav.farskapsportal.consumer.pdf.PdfGeneratorConsumer;
import no.nav.farskapsportal.consumer.pdl.api.KjoennDto;
import no.nav.farskapsportal.consumer.pdl.api.KjoennTypeDto;
import no.nav.farskapsportal.consumer.pdl.api.NavnDto;
import no.nav.farskapsportal.dto.BarnDto;
import no.nav.farskapsportal.dto.DokumentDto;
import no.nav.farskapsportal.dto.DokumentStatusDto;
import no.nav.farskapsportal.dto.ForelderDto;
import no.nav.farskapsportal.dto.SignaturDto;
import no.nav.farskapsportal.persistence.dao.FarskapserklaeringDao;
import no.nav.farskapsportal.persistence.entity.Farskapserklaering;
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
  private static final BarnDto BARN = henteBarn(5);

  @MockBean
  PdfGeneratorConsumer pdfGeneratorConsumer;
  @MockBean
  DifiESignaturConsumer difiESignaturConsumer;
  @MockBean
  PersonopplysningService personopplysningService;
  @Autowired
  private PersistenceService persistenceService;
  @Autowired
  private FarskapserklaeringDao farskapserklaeringDao;
  @Autowired
  private FarskapsportalService farskapsportalService;
  @Autowired
  private FarskapsportalEgenskaper farskapsportalEgenskaper;

  public void ryddeTestdata(String fnrMor, String fnrFar) {
    var nedreGrenseTermindato = LocalDate.now().plusWeeks(farskapsportalEgenskaper.getMinAntallUkerTilTermindato());
    var oevreGrenseTermindato = LocalDate.now().plusWeeks(farskapsportalEgenskaper.getMaksAntallUkerTilTermindato());

    var fes = farskapserklaeringDao.henteFarskapserklaeringer(fnrMor, fnrFar, nedreGrenseTermindato, oevreGrenseTermindato);
    if (!fes.isEmpty()) {
      for (Farskapserklaering fe : fes) {
        farskapserklaeringDao.delete(fe);
      }
    }
  }

  @Nested
  @DisplayName("Teste henteBrukerinformasjon")
  class HenteBrukerinformasjon {

    @Test
    @DisplayName("Mor skal se sine påbegynte og fars ventende farskapserklæringer, og liste over nyfødte uten far")
    void morSkalSeSinePaabegynteOgFarsVentedeFarskapserklaeringerOgListeOverNyfoedteUtenFar() {
      // given
      farskapserklaeringDao.deleteAll();
      var foedselsdatoSpedbarn = LocalDate.now().minusMonths(2).minusDays(21);
      var spedbarnUtenFar = BarnDto.builder().foedselsnummer(foedselsdatoSpedbarn.format(DateTimeFormatter.ofPattern("ddMMyy")) + "10100").build();
      var farskapserklaeringSomManglerSignaturFraMor = henteFarskapserklaering(MOR, FAR, spedbarnUtenFar);
      farskapserklaeringSomManglerSignaturFraMor.getDokument().setPadesUrl(null);

      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setPadesUrl(lageUrl("padesOppdatertVedSigneringMor"));
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now().minusMinutes(3));

      assertAll(() -> assertNull(farskapserklaeringSomManglerSignaturFraMor.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomManglerSignaturFraMor.getDokument().getPadesUrl()),
          () -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getPadesUrl()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      var lagretFarskapserklaeringSomVentePaaMor = persistenceService.lagreFarskapserklaering(farskapserklaeringSomManglerSignaturFraMor);

      var lagretFarskapserklaeringSomVentePaaFar = persistenceService.lagreFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(MOR.getForelderrolle());

      when(personopplysningService.henteNyligFoedteBarnUtenRegistrertFar(MOR.getFoedselsnummer()))
          .thenReturn(Set.of(spedbarnUtenFar.getFoedselsnummer()));

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(MOR.getFoedselsnummer());

      // then
      assertAll(() -> assertTrue(1 == brukerinformasjon.getMorsVentendeFarskapserklaeringer().size()),
          () -> assertTrue(1 == brukerinformasjon.getFnrNyligFoedteBarnUtenRegistrertFar().size()),
          () -> assertTrue(1 == brukerinformasjon.getFarsVentendeFarskapserklaeringer().size()));
    }

    @Test
    @DisplayName("Mor skal se sine påbegynte farskapserklæringer")
    void morSkalSeSinePaabegynteFarskapserklaeringer() {

      // given
      farskapserklaeringDao.deleteAll();
      var farskapserklaeringSomManglerMorsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomManglerMorsSignatur.getDokument().setPadesUrl(null);

      assertAll(() -> assertNull(farskapserklaeringSomManglerMorsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomManglerMorsSignatur.getDokument().getPadesUrl()),
          () -> assertNull(farskapserklaeringSomManglerMorsSignatur.getDokument().getSignertAvFar()));

      var lagretFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaeringSomManglerMorsSignatur);

      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(MOR.getForelderrolle());

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(MOR.getFoedselsnummer());

      // then
      assertAll(() -> assertTrue(1 == brukerinformasjon.getMorsVentendeFarskapserklaeringer().size()),
          () -> assertTrue(0 == brukerinformasjon.getFnrNyligFoedteBarnUtenRegistrertFar().size()),
          () -> assertTrue(0 == brukerinformasjon.getFarsVentendeFarskapserklaeringer().size()));
    }

    @Test
    @DisplayName("Mor skal se farskapserklæringer som venter på far")
    void morSkalSeFarskapserklaeringerSomVenterPaaFar() {

      // given
      farskapserklaeringDao.deleteAll();
      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getPadesUrl()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      var lagretFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(MOR.getFoedselsnummer())).thenReturn(MOR.getForelderrolle());

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(MOR.getFoedselsnummer());

      // then
      assertTrue(1 == brukerinformasjon.getFarsVentendeFarskapserklaeringer().size());
    }

    @Test
    @DisplayName("Far skal se sine ventende farskapserklæringer")
    void farSkalSeSineVentendeFarskapserklaeringer() {
      // given
      farskapserklaeringDao.deleteAll();
      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setSignertAvMor(LocalDateTime.now());

      assertAll(() -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNotNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getPadesUrl()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      var lagretFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(FAR.getForelderrolle());

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(FAR.getFoedselsnummer());

      // then
      assertTrue(1 == brukerinformasjon.getFarsVentendeFarskapserklaeringer().size());
    }

    @Test
    @DisplayName("Far skal ikke se farskapserklæringer som mor ikke har signert")
    void farSkalIkkeSeFarskapserklaeringerSomMorIkkeHarSignert() {
      // given
      farskapserklaeringDao.deleteAll();
      var farskapserklaeringSomVenterPaaFarsSignatur = henteFarskapserklaering(MOR, FAR, BARN);
      farskapserklaeringSomVenterPaaFarsSignatur.getDokument().setPadesUrl(null);

      assertAll(() -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvMor()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getPadesUrl()),
          () -> assertNull(farskapserklaeringSomVenterPaaFarsSignatur.getDokument().getSignertAvFar()));

      var lagretFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaeringSomVenterPaaFarsSignatur);

      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(FAR.getForelderrolle());

      // when
      var brukerinformasjon = farskapsportalService.henteBrukerinformasjon(FAR.getFoedselsnummer());

      // then
      assertAll(() -> assertTrue(0 == brukerinformasjon.getMorsVentendeFarskapserklaeringer().size()),
          () -> assertTrue(0 == brukerinformasjon.getFarsVentendeFarskapserklaeringer().size()),
          () -> assertTrue(0 == brukerinformasjon.getFnrNyligFoedteBarnUtenRegistrertFar().size()));
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
      ryddeTestdata(MOR.getFoedselsnummer(), FAR.getFoedselsnummer());

      // given
      var barn = henteBarn(4);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(FAR.getFornavn()).etternavn(FAR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(FAR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = DokumentDto.builder().dokumentnavn("Farskapserklæering.pdf").innhold("Jeg erklærer med dette farskap til barnet..".getBytes())
          .redirectUrlMor(lageUrl("redirect-mor")).build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when
      var respons = farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskaperklaeringRequest.builder().barn(barn).opplysningerOmFar(opplysningerOmFar).build());

      // then
      assertEquals(pdf.getRedirectUrlMor(), respons.getRedirectUrlForSigneringMor());
    }

    @Test
    @DisplayName("Skal kaste IllegalArgumentException dersom mor og far er samme person")
    void skalKasteIllegalArgumentExceptionDersomMorOgFarErSammePerson() {

      // given
      var barn = henteBarn(4);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(MOR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = DokumentDto.builder().dokumentnavn("Farskapserklæering.pdf").innhold("Jeg erklærer med dette farskap til barnet..".getBytes())
          .redirectUrlMor(lageUrl("redirect-mor")).build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when, then
      assertThrows(IllegalArgumentException.class, () -> farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskaperklaeringRequest.builder().barn(barn).opplysningerOmFar(opplysningerOmFar).build()));
    }

    @Test
    @DisplayName("Skal kaste IllegalArgumentExceptionDersomTermindatoErUgyldig")
    void skalKasteIllegalArgumentExceptionDersomTermindatoErUgyldig() {
      // given
      var barnMedTermindatoForLangtFremITid = henteBarn(farskapsportalEgenskaper.getMaksAntallUkerTilTermindato() + 2);
      var registrertNavnMor = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var registrertNavnFar = NavnDto.builder().fornavn(MOR.getFornavn()).etternavn(MOR.getEtternavn()).build();
      var opplysningerOmFar = KontrollerePersonopplysningerRequest.builder().foedselsnummer(MOR.getFoedselsnummer())
          .navn(registrertNavnFar.getFornavn() + " " + registrertNavnFar.getEtternavn()).build();

      var pdf = DokumentDto.builder().dokumentnavn("Farskapserklæering.pdf").innhold("Jeg erklærer med dette farskap til barnet..".getBytes())
          .redirectUrlMor(lageUrl("redirect-mor")).build();

      when(personopplysningService.henteNavn(MOR.getFoedselsnummer())).thenReturn(registrertNavnMor);
      when(personopplysningService.henteNavn(FAR.getFoedselsnummer())).thenReturn(registrertNavnFar);
      when(pdfGeneratorConsumer.genererePdf(any())).thenReturn(pdf);
      doNothing().when(difiESignaturConsumer).oppretteSigneringsjobb(any(), any(), any());

      // when, then
      assertThrows(IllegalArgumentException.class, () -> farskapsportalService.oppretteFarskapserklaering(MOR.getFoedselsnummer(),
          OppretteFarskaperklaeringRequest.builder().barn(barnMedTermindatoForLangtFremITid).opplysningerOmFar(opplysningerOmFar).build()));
    }
  }

  @Nested
  @DisplayName("Teste henteSignertDokumentEtterRedirect")
  class HenteSignertDokumentEtterRedirect {

    @Test
    @DisplayName("Far skal se dokument etter redirect dersom status query token er gyldig")
    void farSkalSeDokumentEtterRedirectDersomStatusQueryTokenErGyldig() {

      // given
      farskapserklaeringDao.deleteAll();

      var statuslenke = lageUrl("status");
      var farskapserklaering = henteFarskapserklaering(MOR, FAR, BARN);
      var padesFar = lageUrl("padesFar");
      farskapserklaering.getDokument().setSignertAvMor(LocalDateTime.now().minusMinutes(3));

      assertAll(() -> assertNotNull(farskapserklaering.getDokument().getPadesUrl()),
          () -> assertNull(farskapserklaering.getDokument().getSignertAvFar()));

      var lagretFarskapserklaering = persistenceService.lagreFarskapserklaering(farskapserklaering);

      when(personopplysningService.bestemmeForelderrolle(FAR.getFoedselsnummer())).thenReturn(FAR.getForelderrolle());
      when(personopplysningService.henteGjeldendeKjoenn(FAR.getFoedselsnummer())).thenReturn(KjoennDto.builder().kjoenn(KjoennTypeDto.MANN).build());

      when(difiESignaturConsumer.henteDokumentstatusEtterRedirect(any(), any())).thenReturn(
          DokumentStatusDto.builder().statuslenke(statuslenke).erSigneringsjobbenFerdig(true).padeslenke(padesFar).signaturer(List.of(
              SignaturDto.builder().signatureier(FAR.getFoedselsnummer()).harSignert(true).tidspunktForSignering(LocalDateTime.now().minusSeconds(3))
                  .build())).build());

      when(difiESignaturConsumer.henteSignertDokument(any())).thenReturn(farskapserklaering.getDokument().getInnhold());

      // when
      var respons = farskapsportalService.henteSignertDokumentEtterRedirect(FAR.getFoedselsnummer(), "etGyldigStatusQueryToken");

      var oppdatertFarskapserklaering = farskapserklaeringDao.findById(lagretFarskapserklaering.getId()).get();

      // then
      assertAll(() -> assertNotNull(oppdatertFarskapserklaering.getDokument().getSignertAvFar()),
          () -> assertEquals(padesFar, oppdatertFarskapserklaering.getDokument().getPadesUrl()),
          () -> assertEquals(farskapserklaering.getDokument().getInnhold(), respons));
    }
  }
}

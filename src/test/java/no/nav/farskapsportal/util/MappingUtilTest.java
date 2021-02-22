package no.nav.farskapsportal.util;

import static no.nav.farskapsportal.FarskapsportalApplicationLocal.PROFILE_TEST;
import static no.nav.farskapsportal.TestUtils.henteForelder;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import no.nav.farskapsportal.api.Forelderrolle;
import no.nav.farskapsportal.dto.BarnDto;
import no.nav.farskapsportal.dto.DokumentDto;
import no.nav.farskapsportal.dto.FarskapserklaeringDto;
import no.nav.farskapsportal.dto.ForelderDto;
import no.nav.farskapsportal.persistence.entity.Barn;
import no.nav.farskapsportal.persistence.entity.Farskapserklaering;
import no.nav.farskapsportal.persistence.entity.Forelder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@DisplayName("MappingUtilTest")
@SpringBootTest(classes = {MappingUtil.class, ModelMapper.class})
@ActiveProfiles(PROFILE_TEST)
public class MappingUtilTest {

  private static final ForelderDto MOR_DTO = henteForelder(Forelderrolle.MOR);
  private static final ForelderDto FAR_DTO = henteForelder(Forelderrolle.FAR);
  private static final DokumentDto DOKUMENT_DTO = getDokumentDto();
  private static final LocalDate TERMINDATO = LocalDate.now().plusMonths(2).minusDays(13);


  @Autowired
  private MappingUtil mappingUtil;

  private static DokumentDto getDokumentDto() {
    try {
      return DokumentDto.builder().innhold("en farskapserklæring".getBytes(StandardCharsets.UTF_8)).dokumentnavn("Farskapserklæring.pdf")
          .padesUrl(new URI("https://pades.posten.no/")).redirectUrlFar(new URI("https://redirectUrlFar.posten.no/"))
          .redirectUrlMor(new URI("https://redirectUrlMor.posten.no/")).dokumentStatusUrl(new URI("https://dokumentStatusUrl.posten.no/"))
          .signertAvMor(LocalDateTime.now()).signertAvFar(LocalDateTime.now()).build();
    } catch (URISyntaxException uriSyntaxException) {
      uriSyntaxException.printStackTrace();
    }

    return null;
  }

  @Nested
  @DisplayName("Skal mappe mellom DTO og entitet for Barn")
  class BarnMapping {

    @Test
    @DisplayName("Skal mappe barn med termindato til dto")
    void skalMappeBarnMedTermindatoTilDto() {

      // given
      var barn = Barn.builder().termindato(TERMINDATO).build();

      // when
      var barnDto = mappingUtil.toDto(barn);

      // then
      assertEquals(TERMINDATO, barnDto.getTermindato());
    }

    @Test
    @DisplayName("Skal mappe barn med fødselsnummer til dto")
    void skalMappeBarnMedFoedselsnummerTilDto() {

      // given
      var foedselsdato = LocalDate.now().minusMonths(2).minusDays(13);
      var foedselsnummer = foedselsdato.format(DateTimeFormatter.ofPattern("ddMMyy")) + "12345";
      var barn = Barn.builder().foedselsnummer(foedselsnummer).build();

      // when
      var barnDto = mappingUtil.toDto(barn);

      // then
      assertEquals(foedselsnummer, barnDto.getFoedselsnummer());
    }


    @Test
    @DisplayName("Skal mappe barnDto med termindato til entitet")
    void skalMappeBarnDtoMedTermindatoTilEntitiet() {

      // given
      var barnDto = BarnDto.builder().termindato(TERMINDATO).build();

      // when
      var barn = mappingUtil.toEntity(barnDto);

      // then
      assertEquals(TERMINDATO, barn.getTermindato());
    }

    @Test
    @DisplayName("Skal mappe barnDto med fødselsnummer til entitet")
    void skalMappeBarnDtoMedFoedselsnummerTilEntitiet() {

      // given
      var foedselsdato = LocalDate.now().minusMonths(2).minusDays(13);
      var foedselsnummer = foedselsdato.format(DateTimeFormatter.ofPattern("ddMMyy")) + "12345";
      var barnDto = BarnDto.builder().foedselsnummer(foedselsnummer).build();

      // when
      var barn = mappingUtil.toEntity(barnDto);

      // then
      assertEquals(barnDto.getFoedselsnummer(), barn.getFoedselsnummer());
    }
  }

  @Nested
  @DisplayName("Skal mappe mellom DTO og entitet for Forelder")
  class ForelderMapping {

    @Test
    @DisplayName("Skal mappe forelder, DTO til entitet")
    void skalMappeForelderDtoTilEntitet() {

      // given, when
      var forelder = mappingUtil.toEntity(FAR_DTO);

      // then
      assertAll(() -> assertEquals(FAR_DTO.getFoedselsnummer(), forelder.getFoedselsnummer()),
          () -> assertEquals(FAR_DTO.getFornavn(), forelder.getFornavn()), () -> assertEquals(FAR_DTO.getEtternavn(), forelder.getEtternavn()));

    }

    @Test
    @DisplayName("Skal mappe forelder, entitet til DTO")
    void skalMappeForelderEntitetTilDto() {

      // given
      var forelder = Forelder.builder().fornavn("Sponge").etternavn("Bob").foedselsnummer("12345678910").build();

      // when
      var forelderDto = mappingUtil.toDto(forelder);

      // then
      assertAll(() -> assertEquals(forelder.getFoedselsnummer(), forelderDto.getFoedselsnummer()),
          () -> assertEquals(forelder.getFornavn(), forelderDto.getFornavn()),
          () -> assertEquals(forelder.getEtternavn(), forelderDto.getEtternavn()));

    }
  }

  @Nested
  @DisplayName("Skal mappe mellom DTO og entitet for Dokument")
  class DokumentMapping {

    @Test
    @DisplayName("Skal mappe dokument, DTO til entitet")
    void skalMappeDokumentDtoTilEntitet() {

      // given, when
      var dokument = mappingUtil.toEntity(DOKUMENT_DTO);

      // then
      assertAll(() -> assertEquals(DOKUMENT_DTO.getDokumentnavn(), dokument.getDokumentnavn()),
          () -> assertEquals(DOKUMENT_DTO.getDokumentStatusUrl().toString(), dokument.getDokumentStatusUrl()),
          () -> assertTrue(Arrays.equals(DOKUMENT_DTO.getInnhold(), dokument.getInnhold())),
          () -> assertEquals(DOKUMENT_DTO.getPadesUrl().toString(), dokument.getPadesUrl()),
          () -> assertEquals(DOKUMENT_DTO.getSignertAvFar(), dokument.getSignertAvFar()),
          () -> assertEquals(DOKUMENT_DTO.getSignertAvMor(), dokument.getSignertAvMor()),
          () -> assertEquals(DOKUMENT_DTO.getRedirectUrlFar().toString(), dokument.getRedirectUrlFar()),
          () -> assertEquals(DOKUMENT_DTO.getRedirectUrlMor().toString(), dokument.getRedirectUrlMor()));
    }

    @Test
    @DisplayName("Skal mappe dokument, entitet til DTO")
    void skalMappeDokumentEntitetTilDto() {

      // given
      var dokument = mappingUtil.toEntity(DOKUMENT_DTO);

      // when
      var dokumentDto = mappingUtil.toDto(dokument);

      // then
      assertAll(() -> assertEquals(DOKUMENT_DTO.getDokumentStatusUrl(), dokumentDto.getDokumentStatusUrl()),
          () -> assertEquals(DOKUMENT_DTO.getPadesUrl(), dokumentDto.getPadesUrl()),
          () -> assertEquals(DOKUMENT_DTO.getRedirectUrlMor(), dokumentDto.getRedirectUrlMor()),
          () -> assertEquals(DOKUMENT_DTO.getRedirectUrlFar(), dokumentDto.getRedirectUrlFar()),
          () -> assertEquals(DOKUMENT_DTO.getDokumentnavn(), dokumentDto.getDokumentnavn()),
          () -> assertEquals(DOKUMENT_DTO.getSignertAvMor(), dokumentDto.getSignertAvMor()),
          () -> assertEquals(DOKUMENT_DTO.getSignertAvFar(), dokumentDto.getSignertAvFar()),
          () -> assertTrue(Arrays.equals(DOKUMENT_DTO.getInnhold(), dokumentDto.getInnhold())));
    }
  }

  @Nested
  @DisplayName("Skal mappe mellom DTO og entitet for Farskapserklaering")
  class FarskapserklaeringMapping {

    @Test
    @DisplayName("Skal mappe farskapserklaering, DTO til entitet")
    void skalMappeFarskapserklaeringDtoTilEntitet() {

      // given
      var farskapserklaeringDto = FarskapserklaeringDto.builder().far(FAR_DTO).mor(MOR_DTO).barn(BarnDto.builder().termindato(TERMINDATO).build())
          .dokument(DOKUMENT_DTO).build();

      // when
      var farskapserklaering = mappingUtil.toEntity(farskapserklaeringDto);

      // then
      assertAll(() -> assertEquals(FAR_DTO.getFoedselsnummer(), farskapserklaering.getFar().getFoedselsnummer()),
          () -> assertEquals(MOR_DTO.getFoedselsnummer(), farskapserklaering.getMor().getFoedselsnummer()),
          () -> assertEquals(TERMINDATO, farskapserklaering.getBarn().getTermindato()),
          () -> assertTrue(Arrays.equals(DOKUMENT_DTO.getInnhold(), farskapserklaering.getDokument().getInnhold())));
    }

    @Test
    @DisplayName("Skal mappe farskapserklaering - Entitet til DTO")
    void skalMappeFarskapserklaeringEntitetTilDto() {

      // given
      var dokument = mappingUtil.toEntity(DOKUMENT_DTO);
      var far = mappingUtil.toEntity(FAR_DTO);
      var mor = mappingUtil.toEntity(MOR_DTO);
      var farskapserklaering = Farskapserklaering.builder().far(far).mor(mor).dokument(dokument).barn(Barn.builder().termindato(TERMINDATO).build())
          .build();

      // when
      var farskapserklaeringDto = mappingUtil.toDto(farskapserklaering);

      // then
      assertAll(() -> assertEquals(FAR_DTO.getFoedselsnummer(), farskapserklaeringDto.getFar().getFoedselsnummer()),
          () -> assertEquals(MOR_DTO.getFoedselsnummer(), farskapserklaeringDto.getMor().getFoedselsnummer()),
          () -> assertEquals(TERMINDATO, farskapserklaeringDto.getBarn().getTermindato()),
          () -> assertEquals(farskapserklaering.getDokument().getSignertAvMor(), farskapserklaeringDto.getDokument().getSignertAvMor()));

    }

  }
}

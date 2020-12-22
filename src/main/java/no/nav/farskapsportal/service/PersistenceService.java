package no.nav.farskapsportal.service;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import no.nav.farskapsportal.api.Forelderrolle;
import no.nav.farskapsportal.consumer.pdl.api.KjoennTypeDto;
import no.nav.farskapsportal.dto.BarnDto;
import no.nav.farskapsportal.dto.DokumentDto;
import no.nav.farskapsportal.dto.FarskapserklaeringDto;
import no.nav.farskapsportal.dto.ForelderDto;
import no.nav.farskapsportal.exception.FarskapserklaeringMedSammeParterEksistererAlleredeIDatabasenException;
import no.nav.farskapsportal.exception.PersonHarFeilRolleException;
import no.nav.farskapsportal.persistence.dao.BarnDao;
import no.nav.farskapsportal.persistence.dao.DokumentDao;
import no.nav.farskapsportal.persistence.dao.FarskapserklaeringDao;
import no.nav.farskapsportal.persistence.dao.ForelderDao;
import no.nav.farskapsportal.persistence.entity.Barn;
import no.nav.farskapsportal.persistence.entity.Dokument;
import no.nav.farskapsportal.persistence.entity.Farskapserklaering;
import no.nav.farskapsportal.persistence.entity.Forelder;
import no.nav.farskapsportal.persistence.exception.FantIkkeEntititetException;
import org.modelmapper.ModelMapper;

@RequiredArgsConstructor
public class PersistenceService {

  private final FarskapserklaeringDao farskapserklaeringDao;

  private final BarnDao barnDao;

  private final ForelderDao forelderDao;

  private final DokumentDao dokumentDao;

  private final ModelMapper modelMapper;

  public Barn lagreBarn(BarnDto dto) {
    var entity = modelMapper.map(dto, Barn.class);
    return barnDao.save(entity);
  }

  public BarnDto henteBarn(int id) {
    var barn =
        barnDao
            .findById(id)
            .orElseThrow(
                () ->
                    new FantIkkeEntititetException(
                        String.format("Fant ikke barn med id %d i databasen", id)));
    return modelMapper.map(barn, BarnDto.class);
  }

  public Forelder lagreForelder(ForelderDto forelderDto) {
    var forelder = modelMapper.map(forelderDto, Forelder.class);
    return forelderDao.save(forelder);
  }

  public ForelderDto henteForelder(int id) {
    var forelder =
        forelderDao
            .findById(id)
            .orElseThrow(
                () ->
                    new FantIkkeEntititetException(
                        String.format("Fant ingen forelder med id %d i databasen", id)));
    return modelMapper.map(forelder, ForelderDto.class);
  }

  public Dokument lagreDokument(DokumentDto dto) {
    var dokument = modelMapper.map(dto, Dokument.class);
    return dokumentDao.save(dokument);
  }

  public DokumentDto henteDokument(int id) {
    var dokument =
        dokumentDao
            .findById(id)
            .orElseThrow(
                () ->
                    new FantIkkeEntititetException(
                        String.format("Fant ikke dokument med id %d i databasen", id)));
    return modelMapper.map(dokument, DokumentDto.class);
  }

  @Transactional
  public Farskapserklaering lagreFarskapserklaering(FarskapserklaeringDto dto) {
    var nyEllerOppdatertFarskapserklaering = modelMapper.map(dto, Farskapserklaering.class);

    var barnErOppgittMedFoedselsnummer =
        nyEllerOppdatertFarskapserklaering.getBarn().getFoedselsnummer() != null
            && nyEllerOppdatertFarskapserklaering.getBarn().getFoedselsnummer().length() > 10;

    Farskapserklaering eksisterendeFarskapserklaering =
        barnErOppgittMedFoedselsnummer
            ? farskapserklaeringDao.henteUnikFarskapserklaering(
                nyEllerOppdatertFarskapserklaering.getMor().getFoedselsnummer(),
                nyEllerOppdatertFarskapserklaering.getFar().getFoedselsnummer(),
                nyEllerOppdatertFarskapserklaering.getBarn().getFoedselsnummer())
            : farskapserklaeringDao.henteUnikFarskapserklaering(
                nyEllerOppdatertFarskapserklaering.getMor().getFoedselsnummer(),
                nyEllerOppdatertFarskapserklaering.getFar().getFoedselsnummer(),
                nyEllerOppdatertFarskapserklaering.getBarn().getTermindato());

    if (eksisterendeFarskapserklaering == null) {
      var eksisterendeMor =
          forelderDao.henteForelderMedFnr(
              nyEllerOppdatertFarskapserklaering.getMor().getFoedselsnummer());
      var eksisterendeFar =
          forelderDao.henteForelderMedFnr(
              nyEllerOppdatertFarskapserklaering.getFar().getFoedselsnummer());

      if (null != eksisterendeMor) {
        nyEllerOppdatertFarskapserklaering.setMor(eksisterendeMor);
      }
      if (null != eksisterendeMor) {
        nyEllerOppdatertFarskapserklaering.setFar(eksisterendeFar);
      }

      return farskapserklaeringDao.save(nyEllerOppdatertFarskapserklaering);
    } else if (!nyEllerOppdatertFarskapserklaering
        .getDokument()
        .equals(eksisterendeFarskapserklaering.getDokument())) {

      eksisterendeFarskapserklaering.setDokument(nyEllerOppdatertFarskapserklaering.getDokument());
      return farskapserklaeringDao.save(eksisterendeFarskapserklaering);
    }

    throw new FarskapserklaeringMedSammeParterEksistererAlleredeIDatabasenException(
        "Farskasperklæring med samme mor far og barn eksisterer allerede i databasen!");
  }

  public Set<FarskapserklaeringDto> henteFarskapserklaeringer(String foedselsnummer) {
    var farskapserklaeringer =
        farskapserklaeringDao.hentFarskapserklaeringerMedPadeslenke(foedselsnummer);

    return farskapserklaeringer.stream()
        .filter(Objects::nonNull)
        .map(fe -> modelMapper.map(fe, FarskapserklaeringDto.class))
        .collect(Collectors.toSet());
  }

  public Set<FarskapserklaeringDto> henteFarskapserklaeringerEtterRedirect(
      String fnrForelder, Forelderrolle forelderrolle, KjoennTypeDto gjeldendeKjoenn) {
    switch (forelderrolle) {
      case MOR:
        return mapTilDto(
            farskapserklaeringDao.hentFarskapserklaeringerMorUtenPadeslenke(fnrForelder));
      case FAR:
        return mapTilDto(farskapserklaeringDao.hentFarskapserklaeringerMedPadeslenke(fnrForelder));
      case MOR_ELLER_FAR:
        if (KjoennTypeDto.KVINNE.equals(gjeldendeKjoenn)) {
          return mapTilDto(
              farskapserklaeringDao.hentFarskapserklaeringerMorUtenPadeslenke(fnrForelder));
        } else if (KjoennTypeDto.MANN.equals(gjeldendeKjoenn)) {
          return mapTilDto(
              farskapserklaeringDao.hentFarskapserklaeringerMedPadeslenke(fnrForelder));
        }

      default:
        throw new PersonHarFeilRolleException(
            String.format(
                "Foreldrerolle %s er foreløpig ikke støttet av løsningen.", forelderrolle));
    }
  }

  private Set<FarskapserklaeringDto> mapTilDto(Set<Farskapserklaering> farskapserklaeringer) {
    return farskapserklaeringer.stream()
        .filter(Objects::nonNull)
        .map(fe -> modelMapper.map(fe, FarskapserklaeringDto.class))
        .collect(Collectors.toSet());
  }

  private Set<FarskapserklaeringDto> henteFarskapserklaeringVedRedirectMorEllerFar(
      String fnrForelder) {
    var farskapserklaeringer =
        farskapserklaeringDao.hentFarskapserklaeringerMorUtenPadeslenke(fnrForelder);
    if (farskapserklaeringer.stream().filter(Objects::nonNull).count() < 1) {
      return mapTilDto(farskapserklaeringer);
    } else {
      return mapTilDto(farskapserklaeringDao.hentFarskapserklaeringerMedPadeslenke(fnrForelder));
    }
  }
}

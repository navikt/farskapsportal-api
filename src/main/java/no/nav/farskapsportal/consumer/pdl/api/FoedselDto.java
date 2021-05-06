package no.nav.farskapsportal.consumer.pdl.api;

import java.time.LocalDate;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class FoedselDto implements PdlDto {

  LocalDate foedselsdato;
  String foedeland;
  String foedested;
  FolkeregistermetadataDto folkeregistermetadata;
  MetadataDto metadata;
}

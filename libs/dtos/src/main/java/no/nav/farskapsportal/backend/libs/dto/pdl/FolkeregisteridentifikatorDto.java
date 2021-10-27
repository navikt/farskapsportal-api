package no.nav.farskapsportal.backend.libs.dto.pdl;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FolkeregisteridentifikatorDto implements PdlDto {

  String identifikasjonsnummer;
  String status;
  String type;
  MetadataDto metadata;
}

package no.nav.farskapsportal.consumer.pdl.stub;

import lombok.Getter;
import lombok.Value;
import no.nav.farskapsportal.consumer.pdl.api.DoedsfallDto;
import no.nav.farskapsportal.consumer.pdl.api.FolkeregisteridentifikatorDto;

@Value
@Getter
public class HentPersonFolkeregisteridentifikator implements HentPersonSubResponse {

  String response;

  public HentPersonFolkeregisteridentifikator(FolkeregisteridentifikatorDto folkeregisteridentifikatorDto) {
    this.response = buildResponse(folkeregisteridentifikatorDto);
  }

  private String buildResponse(FolkeregisteridentifikatorDto folkeregisteridentifikatorDto) {

    if (folkeregisteridentifikatorDto != null) {

      return String.join(
          "\n",
          " \"folkeregisteridentifikator\": [",
          " {",
          " \"identifikasjonsnummer\": \"" + folkeregisteridentifikatorDto.getIdentifikasjonsnummer() + "\",",
          " \"status\": \"" + folkeregisteridentifikatorDto.getStatus() + "\",",
          " \"type\": \"" + folkeregisteridentifikatorDto.getType() + "\",",
          " \"metadata\": {",
          " \"opplysningsId\": \" 123 \",",
          " \"master\": \"FREG\"",
          " }",
          " }",
          "]");
    } else {
      return String.join("\n", " \"folkeregisteridentifikator\": [", "]");
    }
  }
}

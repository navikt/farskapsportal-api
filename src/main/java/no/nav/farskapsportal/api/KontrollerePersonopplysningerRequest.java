package no.nav.farskapsportal.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Value;

@ApiModel
@Value
@Builder
public class KontrollerePersonopplysningerRequest {
  @ApiModelProperty(value = "Fødselsnummer til personen som sjekkes", position = 1)
  String foedselsnummer;

  @ApiModelProperty(value = "Personens fornavn, alltid påkrevd", position = 2)
  String fornavn;

  @ApiModelProperty(
      value = "Personens mellomnavn, påkrevd hvis dette er registrert i folkeregisteret",
      position = 3)
  String mellomnavn;

  @ApiModelProperty(value = "Personens etternavn, alltid påkrevd", position = 4)
  String etternavn;
}

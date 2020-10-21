package no.nav.farskapsportal.dto;

import io.swagger.annotations.ApiModelProperty;
import java.time.LocalDate;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import no.nav.farskapsportal.persistence.entity.Farskapserklaering;

@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class BarnDto {
  @ApiModelProperty("Barnets termindato")
  private LocalDate termindato;
  @ApiModelProperty("Barnets fødselsnummer hvis tilgjengelig")
  private String foedselsnummer;

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("Barn knyttet til termindato: ").append(termindato);
    return builder.toString();
  }
}

package no.nav.farskapsportal.api;

import io.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@ApiModel
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class BekrefteFarskapResponse {
  private boolean farskapBekreftet;
  private String feilmelding;
}

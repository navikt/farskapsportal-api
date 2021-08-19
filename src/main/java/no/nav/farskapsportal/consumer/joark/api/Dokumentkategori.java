package no.nav.farskapsportal.consumer.joark.api;

public enum Dokumentkategori {
  VEDTAKSBREV("VB"),
  INFOBREV("IB"),
  BREV("B");

  private final String kode;

  Dokumentkategori(String kode) {
    this.kode = kode;
  }

  public String getKode() {
    return this.kode;
  }
}

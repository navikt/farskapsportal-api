-- Table: farskapserklaering

/*
    ALTER TABLE public.farskapserklaering
         DROP COLUMN joark_journalpost_id,
         DROP COLUMN sendt_til_joark;
 */

ALTER TABLE public.farskapserklaering
    ADD COLUMN joark_journalpost_id integer,
    ADD COLUMN sendt_til_joark timestamp without time zone;
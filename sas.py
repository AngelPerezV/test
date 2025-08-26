/* Sort the data by the grouping variables to ensure 'by' group processing works */
proc sort data=historico;
    by contrato folio fecha;
    run;

    /* Process the data and find the last change per month */
    data ultimo_cambio;
        set historico;
            by contrato folio fecha;

                /* A `retain` statement holds the value of a variable from one iteration to the next */
                    retain prev_status prev_fecha;

                        /* Check if the group is new and reset retained variables */
                            if first.contrato or first.folio then do;
                                    prev_status = ' ';
                                            prev_fecha = .;
                                                end;

                                                    /* Check for the status change and update the retained variables */
                                                        if lowcase(status) = 's3' and lowcase(prev_status) in ('s1', 's2') then do;
                                                                fecha_cambio = fecha;
                                                                        mes = month(fecha_cambio);
                                                                                anio = year(fecha_cambio);
                                                                                    end;

                                                                                        /* If this is the last observation in the month for the current contract and folio, then output it */
                                                                                            if last.mes then do;
                                                                                                    output;
                                                                                                        end;

                                                                                                            /* Update the retained variables for the next observation */
                                                                                                                prev_status = status;
                                                                                                                    prev_fecha = fecha;

                                                                                                                        format fecha_cambio date9.;
                                                                                                                        run;
package es.us.lsi.hermes.util;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {

    private static final Logger LOG = Logger.getLogger(Utils.class.getName());

    public static void createCsvFolders(String subFolder) {

        try {
            // Creamos un directorio para contener los CSV generados.
            File directory = new File( subFolder);
            String tempFolderPath = directory.toPath().toString() + File.separator;
            directory.mkdir();

            LOG.log(Level.INFO, "createTempFolder() - Directorio permanente para almacenar los CSV: {0}", tempFolderPath);
        } catch (SecurityException ex) {
            LOG.log(Level.SEVERE, "createTempFolder() - No se ha podido generar el archivo con los datos de todos los eventos y los estados del simulador", ex);
        }
    }
}

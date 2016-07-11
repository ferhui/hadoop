package com.aliyun.fs.oss.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.jar.JarFile;

public final class ClassPath {

    /**
     * The logger of the class.
     */
    private static final Log LOG = LogFactory.getLog(ClassPath.class); 

    /**
     * Instance of {@code JarFileFactory} that contains the {@code JarFile} instances into its cache to release.
     */
    private static final Object JAR_FILE_FACTORY = ClassPath.getJarFileFactory();

    /**
     * Method allowing to get the instance of {@link JarFile} corresponding to a given {@link URL} that could
     * be found into the cache.
     */
    private static final Method GET = ClassPath.getMethodGetByURL();

    /**
     * Method allowing to close a given instance of {@link JarFile} and remove it from the cache to make it available
     * to the GC.
     */
    private static final Method CLOSE = ClassPath.getMethodCloseJarFile();

    /**
     * The urls of the resources corresponding to the ClassPath.
     */
    private final URL[] urls;

    /**
     * Constructs a {@code ClassPath} with the specified urls.
     * @param urls the urls of the resources corresponding to the ClassPath.
     */
    public ClassPath(final URL... urls) {
        this.urls = urls.clone();
    }

    /**
     * Releases all the urls to make it available for the GC.
     */
    public void release() {
        for (final URL url : urls) {
            if (url.getPath().endsWith("jar") || url.getPath().endsWith("zip")) {
                try {
                    CLOSE.invoke(JAR_FILE_FACTORY, GET.invoke(JAR_FILE_FACTORY, url));
                } catch (InvocationTargetException | IllegalAccessException e) {
                   LOG.warn(String.format("Could not close the jar file '%s' used by the classloader", url), e);
                }
            }
        }
    }

    /**
     * Gives the {@link Method} allowing to release a given {@link JarFile} instance.
     * @return The {@link Method} allowing to release a given {@link JarFile} instance.
     */
    private static Method getMethodCloseJarFile() {
        if (JAR_FILE_FACTORY != null) {
            try {
                final Method method = JAR_FILE_FACTORY.getClass().getMethod("close", JarFile.class);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException e) {
                LOG.warn("Could not find the method close of the class JarFileFactory", e);
            }
        }

        return null;
    }

    /**
     * Gives the {@link Method} allowing to access to {@link JarFile} instances of the cache.
     * @return The {@link Method} allowing to access to {@link JarFile} instances of the cache.
     */
    private static Method getMethodGetByURL() {
        if (JAR_FILE_FACTORY != null) {
            try {
                final Method method = JAR_FILE_FACTORY.getClass().getMethod("get", URL.class);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException e) {
               LOG.warn("Could not find the method get of the class JarFileFactory", e);
            }
        }
        return null;
    }

    /**
     * Gives the instance of the singleton {@code JarFileFactory}.
     * @return The instance of the singleton {@code JarFileFactory}.
     */
    private static Object getJarFileFactory() {
        try {
            final Method getInstance = Class.forName(
                    "sun.net.www.protocol.jar.JarFileFactory",
                    true,
                    ClassPath.class.getClassLoader()
            ).getMethod("getInstance");
            getInstance.setAccessible(true);
            return getInstance.invoke(null);
        } catch (NoSuchMethodException e) {
           LOG.warn("Could not find the method getInstance of the class JarFileFactory", e);
        } catch (ClassNotFoundException e) {
           LOG.warn("Could not find the class JarFileFactory", e);
        } catch (IllegalAccessException | InvocationTargetException e) {
           LOG.warn("Could not invoke the method getInstance of the class JarFileFactory", e);
        }
        return null;
    }

}

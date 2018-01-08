package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

/**
 * Returns the credential from {@link GcpOptions}.
 */
public class GcpCredentialsProvider implements CredentialsProvider, Serializable {
  GcpOptions options;

  GcpCredentialsProvider(GcpOptions options) {
    this.options = options;
  }

  public Credentials getCredentials() throws IOException {
    return options.getGcpCredential();
  }
}

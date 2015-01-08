package org.opencredo.mesos;

import com.google.protobuf.ByteString;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;

/**
 * Created by jpartner on 28/11/14.
 */
public class MainRunner {

	public static void main(String[] args) throws Exception {

		Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo
				.newBuilder().setFailoverTimeout(120000).setUser("") // Have
																		// Mesos
																		// fill
																		// in
				// the current user.
				.setName("Rendler Framework (Java)");

		if (System.getenv("MESOS_CHECKPOINT") != null) {
			System.out.println("Enabling checkpoint for the framework");
			frameworkBuilder.setCheckpoint(true);
		}

		Scheduler scheduler = new ExampleScheduler();

		MesosSchedulerDriver driver = null;
		if (System.getenv("MESOS_AUTHENTICATE") != null) {
			System.out.println("Enabling authentication for the framework");

			if (System.getenv("DEFAULT_PRINCIPAL") == null) {
				System.err
						.println("Expecting authentication principal in the environment");
				System.exit(1);
			}

			if (System.getenv("DEFAULT_SECRET") == null) {
				System.err
						.println("Expecting authentication secret in the environment");
				System.exit(1);
			}

			Protos.Credential credential = Protos.Credential
					.newBuilder()
					.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
					.setSecret(
							ByteString.copyFrom(System.getenv("DEFAULT_SECRET")
									.getBytes())).build();

			// frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

			driver = new MesosSchedulerDriver(scheduler,
					frameworkBuilder.build(), args[0], credential);
		} else {
			// frameworkBuilder.setPrincipal("test-framework-java");

			driver = new MesosSchedulerDriver(scheduler,
					frameworkBuilder.build(), args[0]);
		}

		int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

		Thread.sleep(1000000);
		// Ensure that the driver process terminates.
		driver.stop();

		System.exit(status);
	}
}

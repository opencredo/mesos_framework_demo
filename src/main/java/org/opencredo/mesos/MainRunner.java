package org.opencredo.mesos;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;

public class MainRunner {

	public static void main(String[] args) throws Exception {

		Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo
				.newBuilder().setFailoverTimeout(120000).setUser("")
				.setName("demo");

		Scheduler scheduler = new ExampleScheduler();
		MesosSchedulerDriver driver = null;

		driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(),
				args[0]);
		int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

		driver.stop();
		System.exit(status);
	}
}

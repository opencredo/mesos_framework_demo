package org.opencredo.mesos;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;

public class MainRunner {

	public static void main(String[] args) throws Exception {

		Protos.FrameworkInfo.Builder builder = Protos.FrameworkInfo
				.newBuilder().setHostname("172.16.0.2");
		builder.setFailoverTimeout(120000).setUser("")
				.setName("framework-example");

		String path = "/vagrant/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar";

		Protos.CommandInfo.URI uri = Protos.CommandInfo.URI.newBuilder()
				.setValue(path).setExtract(false).build();

		String command = "java -cp /vagrant/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar org.opencredo.mesos.CalculatePiExecutor";
		Protos.CommandInfo piCalculatorCommand = Protos.CommandInfo
				.newBuilder().addUris(uri).setValue(command).build();

		Protos.ExecutorInfo executorCrawl = Protos.ExecutorInfo
				.newBuilder()
				.setExecutorId(
						Protos.ExecutorID.newBuilder().setValue(
								"PICalculatorExecutor"))
				.setCommand(piCalculatorCommand).setName("PICalculator")
				.setSource("java").build();

		Scheduler scheduler = new ExampleScheduler(executorCrawl);
		MesosSchedulerDriver driver = null;

		Protos.FrameworkInfo info = builder.build();
		System.out.println("Framework hostname: " + info.getHostname());
		driver = new MesosSchedulerDriver(scheduler, info, args[0]);
		int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

		driver.stop();
		System.exit(status);
	}
}

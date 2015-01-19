package org.opencredo.mesos;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;

public class MainRunner {

	public static void main(String[] args) throws Exception {

		Protos.FrameworkInfo.Builder builder = Protos.FrameworkInfo
				.newBuilder();
		builder.setFailoverTimeout(120000).setUser("")
				.setName("framework-example");

		String path = "/vagrant/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar";

		Protos.CommandInfo.URI uri = Protos.CommandInfo.URI.newBuilder()
				.setValue(path).setExtract(false).build();

		String commandCrawler = "java -cp /vagrant/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar org.opencredo.mesos.CalculatePiExecutor";
		Protos.CommandInfo commandInfoCrawler = Protos.CommandInfo.newBuilder()
				.addUris(uri).setValue(commandCrawler).build();

		Protos.ExecutorInfo executorCrawl = Protos.ExecutorInfo
				.newBuilder()
				.setExecutorId(
						Protos.ExecutorID.newBuilder().setValue(
								"PICalculatorExecutor"))
				.setCommand(commandInfoCrawler).setName("PICalculator")
				.setSource("java").build();

		Scheduler scheduler = new ExampleScheduler(executorCrawl);
		MesosSchedulerDriver driver = null;

		driver = new MesosSchedulerDriver(scheduler, builder.build(), args[0]);
		int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

		driver.stop();
		System.exit(status);
	}
}

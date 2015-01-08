package org.opencredo.mesos;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ExampleScheduler implements Scheduler {

	private int taskIdCounter = 0;

	private Protos.ExecutorInfo executorHello;

	public ExampleScheduler() {
		String commandHello = "echo hello";
		Protos.CommandInfo commandInfoHello = Protos.CommandInfo.newBuilder()
				.setValue(commandHello).build();

		executorHello = Protos.ExecutorInfo
				.newBuilder()
				.setExecutorId(
						Protos.ExecutorID.newBuilder()
								.setValue("CrawlExecutor"))
				.setCommand(commandInfoHello).setName("Hello Executor")
				.setSource("hello").build();
	}

	@Override
	public void registered(SchedulerDriver schedulerDriver,
			Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
		System.out.println("Registered" + frameworkID);

	}

	@Override
	public void reregistered(SchedulerDriver schedulerDriver,
			Protos.MasterInfo masterInfo) {
		System.out.println("Registered");
	}

	@Override
	public void resourceOffers(SchedulerDriver schedulerDriver,
			List<Protos.Offer> offers) {
		Collection<Protos.OfferID> offerIds = new ArrayList<Protos.OfferID>();
		Collection<Protos.TaskInfo> tasks = new ArrayList<Protos.TaskInfo>();

		for (Protos.Offer offer : offers) {
			System.out.println("Offer");
			System.out.println(offer);

			Protos.TaskID taskId = Protos.TaskID.newBuilder()
					.setValue(Integer.toString(taskIdCounter++)).build();

			Protos.TaskInfo task = Protos.TaskInfo
					.newBuilder()
					.setName("task " + taskId)
					.setTaskId(taskId)
					.setSlaveId(offer.getSlaveId())
					.addResources(
							Protos.Resource
									.newBuilder()
									.setName("cpus")
									.setType(Protos.Value.Type.SCALAR)
									.setScalar(
											Protos.Value.Scalar.newBuilder()
													.setValue(1)))
					.addResources(
							Protos.Resource
									.newBuilder()
									.setName("mem")
									.setType(Protos.Value.Type.SCALAR)
									.setScalar(
											Protos.Value.Scalar.newBuilder()
													.setValue(128)))
					.setData(ByteString.copyFromUtf8("jonas"))
					.setExecutor(Protos.ExecutorInfo.newBuilder(executorHello))
					.build();
			tasks.add(task);
			offerIds.add(offer.getId());

		}
		System.out.println("Scheduling " + tasks);
		schedulerDriver.launchTasks(offerIds, tasks);

	}

	@Override
	public void offerRescinded(SchedulerDriver schedulerDriver,
			Protos.OfferID offerID) {

	}

	@Override
	public void statusUpdate(SchedulerDriver schedulerDriver,
			Protos.TaskStatus taskStatus) {

	}

	@Override
	public void frameworkMessage(SchedulerDriver schedulerDriver,
			Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {

	}

	@Override
	public void disconnected(SchedulerDriver schedulerDriver) {

	}

	@Override
	public void slaveLost(SchedulerDriver schedulerDriver,
			Protos.SlaveID slaveID) {

	}

	@Override
	public void executorLost(SchedulerDriver schedulerDriver,
			Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {

	}

	@Override
	public void error(SchedulerDriver schedulerDriver, String s) {

	}
}

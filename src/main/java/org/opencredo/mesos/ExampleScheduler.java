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
	private Protos.ExecutorInfo executorInfo;

	public ExampleScheduler(Protos.ExecutorInfo executorInfo) {
		this.executorInfo = executorInfo;
	}

	@Override
	public void registered(SchedulerDriver schedulerDriver,
			Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
		System.out.println("Registered" + frameworkID);
	}

	@Override
	public void reregistered(SchedulerDriver schedulerDriver,
			Protos.MasterInfo masterInfo) {
		System.out.println("Re-registered");
	}

	@Override
	public void resourceOffers(SchedulerDriver schedulerDriver,
			List<Protos.Offer> offers) {

		for (Protos.Offer offer : offers) {
			// System.out.println("Offer");
			// System.out.println(offer);


			Protos.TaskID taskId = buildNewTaskID();
			Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
					.setName("task " + taskId).setTaskId(taskId)
					.setSlaveId(offer.getSlaveId())
					.addResources(buildResource("cpus", 1))
					.addResources(buildResource("mem", 128))
					.setData(ByteString.copyFromUtf8("" + taskIdCounter))
					.setExecutor(Protos.ExecutorInfo.newBuilder(executorInfo))
					.build();

			launchTask(schedulerDriver, offer, task);
		}
	}

	private void launchTask(SchedulerDriver schedulerDriver,
			Protos.Offer offer, Protos.TaskInfo task) {
		Collection<Protos.TaskInfo> tasks = new ArrayList<Protos.TaskInfo>();
		Collection<Protos.OfferID> offerIDs = new ArrayList<Protos.OfferID>();
		// System.out.println("Scheduling " + task);
		tasks.add(task);
		offerIDs.add(offer.getId());
		schedulerDriver.launchTasks(offerIDs, tasks);
	}

	private Protos.TaskID buildNewTaskID() {
		return Protos.TaskID.newBuilder()
				.setValue(Integer.toString(taskIdCounter++)).build();
	}

	private Protos.Resource buildResource(String name, double value) {
		return Protos.Resource.newBuilder().setName(name)
				.setType(Protos.Value.Type.SCALAR)
				.setScalar(buildScalar(value)).build();
	}

	private Protos.Value.Scalar.Builder buildScalar(double value) {
		return Protos.Value.Scalar.newBuilder().setValue(value);
	}

	@Override
	public void offerRescinded(SchedulerDriver schedulerDriver,
			Protos.OfferID offerID) {
		System.out.println("This offer's been rescinded. Tough luck, cowboy.");
	}

	@Override
	public void statusUpdate(SchedulerDriver schedulerDriver,
			Protos.TaskStatus taskStatus) {
		System.out.println("Status update: " + taskStatus.getState() + " from "
				+ taskStatus.getTaskId().getValue());
	}
	@Override
	public void frameworkMessage(SchedulerDriver schedulerDriver,
			Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
		System.out.println("Received message (scheduler): " + new String(bytes)
				+ " from " + executorID.getValue());
	}

	@Override
	public void disconnected(SchedulerDriver schedulerDriver) {
		System.out.println("We got disconnected yo");
	}

	@Override
	public void slaveLost(SchedulerDriver schedulerDriver,
			Protos.SlaveID slaveID) {
		System.out.println("Lost slave: " + slaveID);
	}

	@Override
	public void executorLost(SchedulerDriver schedulerDriver,
			Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
		System.out.println("Lost executor on slave " + slaveID);
	}

	@Override
	public void error(SchedulerDriver schedulerDriver, String s) {
		System.out.println("We've got errors, man: " + s);
	}
}

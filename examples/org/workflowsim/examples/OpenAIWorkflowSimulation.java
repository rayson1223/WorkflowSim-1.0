package examples;

import java.io.File;
import java.text.DecimalFormat;
import java.util.*;

import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;


public class OpenAIWorkflowSimulation {

    public static void main(String[] args) {
        try {
            // First step: Initialize the WorkflowSim package.
            /**
             * Configurations
             *
             * vmNum            : However, the exact number of vms may not necessarily be vmNum If
             *                      the data center or the host doesn't have sufficient resources the
             *                      exact vmNum would be smaller than that. Take care.
             * cluster_choice   : NONE, HORIZONTAL, VERTICAL, BALANCED
             * cSize            : Any integer > 0
             * dax_path         : path for the dax file
             */
            int vmNum = 30;//number of vms;
            int cSize = 0;
            String clusterChoice = "NONE";
            String daxPath = "WorkflowSim-1.0/config/dax/Montage_1000.xml";
            if(args.length == 4){
                vmNum = Integer.parseInt(args[0]);
                clusterChoice = args[1].toUpperCase();
                cSize = Integer.parseInt(args[2]);
                daxPath = args[3];
            } else {
                org.cloudbus.cloudsim.Log.printLine(
                        "Warning: Please provide the argument according (vmNum, clustering method, cSize, dax path)!"
                );
                return;
            }

            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                org.cloudbus.cloudsim.Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            /**
             * Place to chooce scheduler, override this to our own algo
             */
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.MINMIN;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.LOCAL;

            /**
             * clustering delay must be added, if you don't need it, you can set
             * all the clustering delay to be zero, but not null
             */
            Map<Integer, org.workflowsim.utils.DistributionGenerator> clusteringDelay = new HashMap();
            Map<Integer, org.workflowsim.utils.DistributionGenerator> queueDelay = new HashMap();
            Map<Integer, org.workflowsim.utils.DistributionGenerator> postscriptDelay = new HashMap();
            Map<Integer, org.workflowsim.utils.DistributionGenerator> engineDelay = new HashMap();
            /**
             * Montage has at most 11 horizontal levels
             */
            int maxLevel = 11;
            int interval = 5;
            for (int level = 0; level < maxLevel; level++) {
                org.workflowsim.utils.DistributionGenerator cluster_delay = new org.workflowsim.utils.DistributionGenerator(org.workflowsim.utils.DistributionGenerator.DistributionFamily.WEIBULL, 1.0, 1.0);
                clusteringDelay.put(level, cluster_delay);
                org.workflowsim.utils.DistributionGenerator queue_delay = new org.workflowsim.utils.DistributionGenerator(org.workflowsim.utils.DistributionGenerator.DistributionFamily.WEIBULL, 10.0, 1.0);
                queueDelay.put(level, queue_delay);
                org.workflowsim.utils.DistributionGenerator postscript_delay = new org.workflowsim.utils.DistributionGenerator(org.workflowsim.utils.DistributionGenerator.DistributionFamily.WEIBULL, 30.0, 1.0);
                postscriptDelay.put(level, postscript_delay);
                org.workflowsim.utils.DistributionGenerator engine_delay = new org.workflowsim.utils.DistributionGenerator(org.workflowsim.utils.DistributionGenerator.DistributionFamily.WEIBULL, 50.0, 1.0);
                engineDelay.put(level, engine_delay);
            }

            // Add clustering delay to the overhead parameters
            OverheadParameters op = new OverheadParameters(interval, engineDelay, queueDelay, postscriptDelay, clusteringDelay, 0);

            ClusteringParameters.ClusteringMethod method = null;
            switch (clusterChoice){
                case "NONE":
                    method = ClusteringParameters.ClusteringMethod.NONE;
                    break;
                case "HORIZONTAL":
                    method = ClusteringParameters.ClusteringMethod.HORIZONTAL;
                    break;
                case "VERTICAL":
                    method = ClusteringParameters.ClusteringMethod.VERTICAL;
                    break;
                case "BALANCED":
                    method = ClusteringParameters.ClusteringMethod.BALANCED;
                    break;
                default:
                    method = ClusteringParameters.ClusteringMethod.NONE;
                    break;
            }
            ClusteringParameters cp = new ClusteringParameters(cSize, 0, method, null);

            /**
             * Initialize static parameters
             */
            Parameters.init(vmNum, daxPath, null, null, op, cp, sch_method, pln_method,null, 0);
            ReplicaCatalog.init(file_system);

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            List<org.workflowsim.CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());

            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);

            CloudSim.startSimulation();
            List<org.workflowsim.Job> outputList0 = wfEngine.getJobsReceivedList();
            CloudSim.stopSimulation();
            printJobList(outputList0);
            printResult(outputList0, vmNum);
        } catch (Exception e) {
            e.printStackTrace();
            org.cloudbus.cloudsim.Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    protected static void printResult(List<org.workflowsim.Job> list, int vmNum) {
        int size = list.size();
        org.workflowsim.Job job;
        DecimalFormat dft = new DecimalFormat("###.##");
        double cost = 0.0;
        double totalQueueDelay = 0.0;
        double totalPostscriptedDelay = 0.0;
        double totalMakespan = 0.0;
        double totalExecTime = 0.0;
        for (int i = 0; i < size; i++) {
            job = list.get(i);
            cost += job.getProcessingCost();
            totalMakespan += job.getMakespan();
            totalExecTime += job.getActualCPUTime();
            totalQueueDelay += job.getQueueDelay();
            totalPostscriptedDelay += job.getPostDelay();
        }
        org.cloudbus.cloudsim.Log.printLine(
                dft.format(totalMakespan) + " " +
                dft.format(totalQueueDelay) + " " +
                dft.format(totalExecTime) + " " +
                dft.format(totalPostscriptedDelay) + " " +
                dft.format(cost) + " " +
                vmNum
        );
    }

    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static void printJobList(List<org.workflowsim.Job> list) {
        int size = list.size();
        org.workflowsim.Job job;

        String indent = "    ";
        org.cloudbus.cloudsim.Log.printLine();
        org.cloudbus.cloudsim.Log.printLine("========== OUTPUT ==========");
        org.cloudbus.cloudsim.Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent + "Time" + indent
                + "Start Time" + indent + "Finish Time" + indent + "Estimated Finish Time"
                + indent + "Depth" + indent + "Cost" + indent
                + "Queue Delay" + indent + "File TrasferTime" + indent + "Postscripted Time"
        );

        DecimalFormat dft = new DecimalFormat("###.##");
        double cost = 0.0;
        double totalQueueDelay = 0.0;
        double totalPostscriptedDelay = 0.0;
        double totalMakespan = 0.0;
        double totalExecTime = 0.0;
        for (int i = 0; i < size; i++) {
            job = list.get(i);
            org.cloudbus.cloudsim.Log.print(indent + job.getCloudletId() + indent + indent);

            cost += job.getProcessingCost();
            totalMakespan += job.getMakespan();
            totalExecTime += job.getActualCPUTime();
            totalQueueDelay += job.getQueueDelay();
            totalPostscriptedDelay += job.getPostDelay();
            if (job.getCloudletStatus() == org.cloudbus.cloudsim.Cloudlet.SUCCESS) {
                org.cloudbus.cloudsim.Log.print("SUCCESS");
                org.cloudbus.cloudsim.Log.printLine(
                        indent + indent + job.getResourceId()
                        + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime())
                        + indent + indent + indent + dft.format(job.getFinishTime())
                        + indent + indent + dft.format(job.getEstimatedFinishTime())
                        + indent + indent + indent + job.getDepth()
                        + indent + indent + indent + dft.format(job.getProcessingCost())
                        + indent + indent + indent + dft.format(job.getQueueDelay())
                        + indent + indent + indent + dft.format(job.getFileTransferTime())
                        + indent + indent + indent + dft.format(job.getPostDelay())
                );
            } else if (job.getCloudletStatus() == org.cloudbus.cloudsim.Cloudlet.FAILED) {
                org.cloudbus.cloudsim.Log.print("FAILED");
                org.cloudbus.cloudsim.Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth()
                        + indent + indent + indent + dft.format(job.getProcessingCost()));
            }
        }
        org.cloudbus.cloudsim.Log.printLine("The total cost is " + dft.format(cost));
        org.cloudbus.cloudsim.Log.printLine("The total exec_time is " + dft.format(totalExecTime));
        org.cloudbus.cloudsim.Log.printLine("The total make span is " + dft.format(totalMakespan));
        org.cloudbus.cloudsim.Log.printLine("The total queue delay is " + dft.format(totalQueueDelay));
        org.cloudbus.cloudsim.Log.printLine("The total postscripted delay is " + dft.format(totalPostscriptedDelay));
    }

    protected static List<org.workflowsim.CondorVM> createVM(int userId, int vms) {

        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<org.workflowsim.CondorVM> list = new LinkedList<>();

        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        org.workflowsim.CondorVM[] vm = new org.workflowsim.CondorVM[vms];

        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new org.workflowsim.CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new org.cloudbus.cloudsim.CloudletSchedulerDynamicWorkload(mips * ratio, pesNumber));
            list.add(vm[i]);
        }

        return list;
    }

    protected static WorkflowDatacenter createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<org.cloudbus.cloudsim.Host> hostList = new ArrayList<>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        for (int i = 1; i <=1000 ; i++) {
            List<org.cloudbus.cloudsim.Pe> peList1 = new ArrayList<>();
            int mips = 2000;
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            peList1.add(new org.cloudbus.cloudsim.Pe(0, new org.cloudbus.cloudsim.provisioners.PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
            peList1.add(new org.cloudbus.cloudsim.Pe(1, new org.cloudbus.cloudsim.provisioners.PeProvisionerSimple(mips)));

            int hostId = 0;
            int ram = 2048; //host memory (MB)
            long storage = 1000000; //host storage
            int bw = 10000;
            hostList.add(
                    new org.cloudbus.cloudsim.Host(
                            hostId,
                            new org.cloudbus.cloudsim.provisioners.RamProvisionerSimple(ram),
                            new org.cloudbus.cloudsim.provisioners.BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new org.cloudbus.cloudsim.VmSchedulerTimeShared(peList1))); // This is our first machine
            //hostId++;
        }

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        LinkedList<org.cloudbus.cloudsim.Storage> storageList = new LinkedList<>();	//we are not adding SAN devices by now
        WorkflowDatacenter datacenter = null;

        org.cloudbus.cloudsim.DatacenterCharacteristics characteristics = new org.cloudbus.cloudsim.DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw

        try {
            // Here we set the bandwidth to be 15MB/s
            org.cloudbus.cloudsim.HarddriveStorage s1 = new org.cloudbus.cloudsim.HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter(name, characteristics, new org.cloudbus.cloudsim.VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

}

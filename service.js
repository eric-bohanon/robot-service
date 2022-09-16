const amqp = require("amqplib/callback_api");
const log = require("log");

const ROBOT_CONFIGS = {};
const TASK_DB = {};
// Used for fetching data efficiently for displaying on clients
const APP_CACHE = {};

export default function run(task) {
  log.info("task run completed for following tasks:\n" + JSON.stringify(task));
  sendTasks(task)
    .then(
      log.info(
        "task run completed for following task:\n" + JSON.stringify(task)
      )
    )
    .catch(
      log.error("Error occurred while queueing tasks:\n" + JSON.stringify(task))
    );
}

async function sendTasks(task) {
  const taskMessage = createMessage(task);
  try {
    await sendMessage(task);
  } catch (e) {
    log.error(e);
    await writeTaskToDB(task, { error: e, status: "failed" });
    throw e;
  }

  await writeTaskToDB(task, { status: "queued" });
}

function createMessage(task) {
  return {
    ...task,
    ...getRobotConfigForTask(task),
  };
}

function getRobotConfigForTask(task) {
  return ROBOT_CONFIGS[task.company_id];
}

async function writeTaskToDB(task, extra_data) {
  APP_CACHE[task.id] = { ...task, extraData: extra_data };
  TASK_DB["robot_task_table"][task.robot_id][task.id] = {
    ...task,
    extraData: extra_data,
  };
}

async function sendMessage(taskMessage) {
  // Call the queue API
  // In this case, stole example from RabbitMQ page and modified it to be a priority queue
  amqp.connect("amqp://localhost", function (error0, connection) {
    if (error0) {
      throw error0;
    }
    connection.createChannel(function (error1, channel) {
      if (error1) {
        throw error1;
      }

      // This should go to a specific robot fleet with a specific priority,
      // the consumers will read highest priority queues first, and if empty
      // will fall back to lower priority queues.
      var queue = taskMessage.priority + taskMessage.robot_fleet_id;
      var msg = JSON.stringify(taskMessage);

      args = new HashMap();
      args.put("x-max-priority", 10);
      channel.queueDeclare(queue, true, false, false, args);

      channel.assertQueue(queue, {
        durable: false,
      });
      channel.sendToQueue(queue, Buffer.from(msg), {
        priority: taskMessage.time,
      });
    });
    setTimeout(function () {
      connection.close();
      process.exit(0);
    }, 500);
  });
}

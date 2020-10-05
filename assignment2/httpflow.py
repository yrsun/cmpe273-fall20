import yaml
import sys
import schedule
import time
import requests

response = None


def printAttr(attr, rList, marker):
    marker += 1
    if len(rList) == marker + 1:
        try:
            print (attr.get(rList[marker]))
        except AttributeError:
            print(getattr(response, rList[marker]))
    else:
        printAttr(getattr(response, rList[marker]), rList, marker)


def printData(data):
    if data == "Error":
        print("Error")
    else:
        rList = data.split(".")
        assert rList[0] == "http" and rList[1] == "response"
        if rList[2] == "body":
            rList[2] = "content"
        printAttr(response, rList, 1)


def checkIf(cond):
    if "equal" in cond.keys():
        if cond["equal"]["left"] == "http.response.code":
            assert response is not None
            if response.status_code == cond["equal"]["right"]:
                return True
            else:
                return False
        else:
            print("Equal other than response code unsupported")
            sys.exit(0)
    else:
        print("Condition other than equal unsupported")
        sys.exit(0)


def action(cond):
    if cond["action"] == "::print":
        printData(cond["data"])
    elif "::invoke:step" in cond["action"]:
        iStep = cond["action"].split(":")
        job(steps[int(iStep[4])-1], int(iStep[4]), cond["data"])
    else:
        print("Action other than print or invoke unsupported")
        sys.exit(0)


def checkCondition(condition):
    if checkIf(condition['if']):
        action(condition['then'])
    else:
        action(condition['else'])


def job(steps, stepNum, url):
    step = steps[stepNum]
    global response
    if step['type'] == "HTTP_CLIENT" and step["method"] == "GET":
        if step["outbound_url"] == "::input:data":
            response = requests.get(url)
        else:
            response = requests.get(step["outbound_url"])
        checkCondition(step['condition'])
    else:
        print("error type method")


def get_yaml_schedule(docs):
    minute, hour, day = docs["Scheduler"]["when"].split()
    sche_code = ((minute=='*')<<2) | ((hour=='*')<<1) | (day=='*')
    sche_day = schedule.every().day

    if len(minute) < 2:
        minute = "0" + minute
    if len(hour) < 2:
        hour = "0" + hour
    if day == 0 or day == 7:
        sche_day = schedule.every().sunday
    elif day == 1:
        sche_day = schedule.every().monday
    elif day == 2:
        sche_day = schedule.every().tuesday
    elif day == 3:
        sche_day = schedule.every().wednesday
    elif day == 4:
        sche_day = schedule.every().thursday
    elif day == 5:
        sche_day = schedule.every().friday
    elif day == 6:
        sche_day = schedule.every().saturday

    if sche_code == 7:  # * * *
        print('error: * * *')
    elif sche_code == 6:    # * * a
        sche_day = sche_day
    elif sche_code == 5:    # * a *
        sche_day = sche_day.at('%s:00'%(hour))
    elif sche_code == 4:    # * a a
        sche_day = sche_day.at('%s:00'%(hour))
    elif sche_code == 3:    # a * *
        sche_day = schedule.every(int(minute)).minutes
    elif sche_code == 2:    # a * a
        sche_day = sche_day.at('00:%s'%(minute))
    elif sche_code == 1:    # a a *
        sche_day = sche_day.at('%s:%s'%(hour, minute))
    elif sche_code == 0:    # a a a
        sche_day = sche_day.at('%s:%s'%(hour, minute))

    return sche_day


with open(sys.argv[1], 'r') as yaml_file:
    docs = yaml.safe_load(yaml_file)
sche_day = get_yaml_schedule(docs)
steps = docs["Steps"]
job(steps[0], 1, "")
step_id_to_execute = docs["Scheduler"]["step_id_to_execute"]
for i in step_id_to_execute:
    sche_day.do(job, steps[i-1], 1, "")


while True:
    schedule.run_pending()
    time.sleep(1)


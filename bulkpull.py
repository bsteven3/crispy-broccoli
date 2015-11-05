from collections import deque
import datetime
from Queue import Queue
import requests
from simple_salesforce import SalesforceLogin
import threading
import xmltodict

class ThreadedBulkPull(object):
    """Multithreaded solution for pulling large record sets from SFDC

    After creating a job, a submitted batch will be automatically split into
    an appropriate number of subsequent batches by the API. The produced batches
    are monitored by a thread pool, and their associated return data is
    downloaded when available. Currently retrieved data is dumped to a file
    on disk, but this could easily be linked to a data warehouse instead.
    """
    STATE_CLOSED = 'Closed'

    api_ver = "35.0"
    session_id = None
    instance = None
    job_id = None

    sfdcXml = {
        'job': {
            'url': "https://{instance}.salesforce.com/services/async/{api_ver}/job",
            'body': "<?xml version='1.0' encoding='UTF-8'?>
                <jobInfo xmlns='http://www.force.com/2009/06/asyncapi/dataload'>
                    <operation>query</operation>
                    <object>Contact</object>
                    <contentType>CSV</contentType>
                </jobInfo>",
            'headers': {"Content-Type": "application/xml; charset=UTF-8"},
        },
        'batch': {
            'url': "https://{instance}.salesforce.com/services/async/{api_ver}/job/{job_id}/batch
",
            'body': None,
            'headers': {"Content-Type": "text/csv; charset=UTF-8; Sforce-Enable-PKChunking: chunkSize={chunk_size}"},
        },
        'check': {
            'batchList': {
                'url': "https://{instance}.salesforce.com/services/async/{api_ver}/job/{job_id}/batch",
            },
            'job': {
                'url': "https://{instance}.salesforce.com/services/async/{api_ver}/job/{job_id}",
            },
            'batch': {
                'url': "https://{instance}.salesforce.com/services/async/{api_ver}/job/{job_id}/batch/{batch_id}"
            },
        },
        'retrieveUrl': {
            'result_id': "https://{instance}.salesforce.com/services/async/{api_ver}/job/{job_id}/batch/{batch_id}/result",
            'data': "https://{instance}.salesforce.com/services/async/{api_ver}/job/{job_id}/batch/{batch_id}/result/{result_id}",
        },
        'jobClose': {
            'url': "https://{instance}.salesforce.com/services/async/{api_ver}/job/{job_id}",
            'body': """xml version="1.0" encoding="UTF-8"?>
                <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                  <state>Closed</state>
                </jobInfo>""",
            'headers': {"Content-Type": "application/xml; charset=UTF-8"},
        },
    }

    session_args = {
        'api_ver': self.api_ver,
        'session_id': self.session_id,
        'instance': self.instance,
        'job_id': self.job_id,
        'sfdcXml': self.sfdcXml,
    }

    def __init__(self):
        """Doesn't create any vendor-specific state in case this needs
        to be extended to use other CRMs
        """
        pass

    ##  SFDC methods    ##
    @staticmethod
    def sfdcApiLogin(username, password, token):
        """Returns SFDC session id and instance domain needed for API calls
        param:  username
        type:   str, SFDC login username
        param:  password
        type:   str, SFDC login password
        param:  token
        type:   str, SFDC login token
        return: session_id and instance domain
        type:   tuple
        """
        session_id, instance = SalesforceLogin(username, password, token)
        return session_id, instance

    @staticmethod
    def sfdcCreateJob(**kwargs):
        """Creates a job which will contain all batches requested
        relies on class instance's session_id and instance domain
        kwargs:  expects self.session_args
        return: job_id
        type:   str
        """
        api_ver = kwargs.get('api_ver', '')
        session_id = kwargs.get('session_id', '')
        instance = kwargs.get('instance', '')
        job_id = kwargs.get('job_id', '')
        sfdcXml = kwargs.get('sfdcXml', {})

        bodyXml = sfdcXml.get('job', {}).get('body')
        url = sfdcXml.get('job', {}).get('url')
        headers = sfdcXml.get('job', {}).get('headers')

        bodyXml = unicode(bodyXml, "utf-8")
        url = url.format(instance=instance, api_ver=api_ver)
        headers['X-SFDC-Session'] = self.session_id

        resp = requests.post(url=url, headers=headers, data=bodyXml)
        dictResp = xmltodict.parse(resp.text)
        job_id = str(dictResp['jobInfo']['id'])

        self.job_id = job_id
        return job_id

    @staticmethod
    def sfdcCreateBatch(query, chunk_size=10000, **kwargs):
        """Creates a batch under the class instance's job given a query.
        A very general query should be used as the Bulk API will split
        it into a number of other batches based on chunk size. Accordingly,
        this should only need to be run once.

        param:  query
        type:   str, SFDC SOQL query
        param:  chunk_size
        type:   int
        kwargs:  expects self.session_args
        return: batch_id
        type:   str
        """
        api_ver = kwargs.get('api_ver', '')
        session_id = kwargs.get('session_id', '')
        instance = kwargs.get('instance', '')
        job_id = kwargs.get('job_id', '')
        sfdcXml = kwargs.get('sfdcXml', {})

        bodyXml = sfdcXml.get('batch', {}).get('body')
        url = sfdcXml.get('batch', {}).get('url')
        headers = sfdcXml.get('batch', {}).get('headers')

        bodyXml = unicode(query, "UTF-8")
        url = url.format(instance=instance, api_ver=api_ver,\
                         job_id=job_id)
        headers['Content-Type'] = headers.get('Content-Type', '')\
                                  .format(chunk_size=chunk_size)
        headers['X-SFDC-Session'] = session_id

        resp = requests.post(url=url, headers=headers, data=bodyXml)
        dictResp = xmltodict.parse(resp.text)
        batch_id = str(dictResp['batchInfo']['id'])

        return batch_id

    @staticmethod
    def sfdcGetBatches(**kwargs):
        """Gets a list of all batches created for a job. As we rely
        on the API to break a single query into a number of batches, this
        is the only way to see all the batches assigned to a job.
        kwargs:  expects self.session_args
        return: batch_ids
        type:   list
        """
        api_ver = kwargs.get('api_ver', '')
        session_id = kwargs.get('session_id', '')
        instance = kwargs.get('instance', '')
        job_id = kwargs.get('job_id', '')
        sfdcXml = kwargs.get('sfdcXml', {})

        url = sfdcXml.get('check', {}).get('batchList', {}).get('url', '')

        headers = {'X-SFDC-Session': session_id}
        url = url.format(instance=instance, api_ver=api_ver,\
                         job_id=job_id)

        resp = requests.post(url=url, headers=headers)
        dictResp = xmltodict.parse(resp.text)

        batch_ids = [batch['id'] for batch\
                     in dictResp['batchInfoList']['batchInfo']]

        return batch_ids

    @staticmethod
    def sfdcJobStatus(**kwargs):
        """Returns a job's status
        kwargs:  expects self.session_args
        """
        api_ver = kwargs.get('api_ver', '')
        session_id = kwargs.get('session_id', '')
        instance = kwargs.get('instance', '')
        job_id = kwargs.get('job_id', '')
        sfdcXml = kwargs.get('sfdcXml', {})

        url = sfdcXml.get('check', {}).get('job', {}).get('url')

        headers = {'X-SFDC-Session': session_id}
        url = url.format(instance=instance, api_ver=api_ver,\
                         job_id=job_id)

        resp = requests.post(url=url, headers=headers)
        dictResp = xmltodict.parse(resp.text)
        job_status = {
            'state': str(dictResp['jobInfo']['state']),
            'batches_queued': int(dictResp['jobInfo']['numberBatchesQueued'],
            'batches_inprog': int(dictResp['jobInfo']['numberBatchesInProgress'],
            'batches_failed': int(dictResp['jobInfo']['numberBatchesFailed'],
            'batches_finish': int(dictResp['jobInfo']['numberBatchesCompleted'],
            'batches_total': int(dictResp['jobInfo']['numberBatchesTotal'],
        }

        return job_status

    @staticmethod
    def sfdcBatchStatus(batch_id, **kwargs):
        """Returns a batch's status
        param:  batch_id
        type:   str
        kwargs:  expects self.session_args
        """
        api_ver = kwargs.get('api_ver', '')
        session_id = kwargs.get('session_id', '')
        instance = kwargs.get('instance', '')
        job_id = kwargs.get('job_id', '')
        sfdcXml = kwargs.get('sfdcXml', {})

        url = sfdcXml.get('check', {}).get('batch', {}).get('url')

        headers = {'X-SFDC-Session': session_id}
        url = url.format(instance=instance, api_ver=api_ver,\
                         job_id=job_id, batch_id=batch_id)

        resp = requests.post(url=url, headers=headers)
        dictResp = xmltodict.parse(resp.text)
        batch_status = {
            'state': str(dictResp['batchInfo']['state']),
            'recs_proc': int(dictResp['batchInfo']['numberRecordsProcessed'],
            'recs_fail': int(dictResp['batchInfo']['numberRecordsFailed'],
        }

        return batch_status

    @staticmethod
    def sfdcRetrieveData(batch_id, **kwargs):
        """Retrieves results from query.
        param:  batch_id
        type:   str
        kwargs:  expects self.session_args
        """
        api_ver = kwargs.get('api_ver', '')
        session_id = kwargs.get('session_id', '')
        instance = kwargs.get('instance', '')
        job_id = kwargs.get('job_id', '')
        sfdcXml = kwargs.get('sfdcXml', {})

        url = sfdcXml.get('retrieveUrl', {}).get('result_id', '')

        headers = {'X-SFDC-Session': session_id}
        url = url.format(instance=instance, api_ver=api_ver,\
                         job_id=job_id, batch_id=batch_id)

        resp = requests.post(url=url, headers=headers)
        dictResp = xmltodict.parse(resp.text)
        result_id = dictResp['result-list']['result']

        url = url.format(instance=instance, api_ver=api_ver,\
                         job_id=job_id, batch_id=batch_id, result_id=result_id)

        data = requsts.post(url=url, headers=headers)

        return data

    @staticmethod
    def sfdcCloseJob(**kwargs):
        """Close job after all batches are completed so API knows to begin
        work on associated batches.
        kwargs:  expects self.session_args
        return: success/failure
        type: bool
        """
        api_ver = kwargs.get('api_ver', '')
        session_id = kwargs.get('session_id', '')
        instance = kwargs.get('instance', '')
        job_id = kwargs.get('job_id', '')
        sfdcXml = kwargs.get('sfdcXml', {})

        bodyXml = sfdcXml.get('jobClose', {}).get('body')
        url = sfdcXml.get('jobClose', {}).get('url')
        headers = sfdcXml.get('jobClose', {}).get('headers')

        bodyXml = unicode(bodyXml, "utf-8")
        url = url.format(instance=instance, api_ver=api_ver, job_id=job_id)
        headers['X-SFDC-Session'] = self.session_id

        resp = requests.post(url=url, headers=headers, data=bodyXml)
        dictResp = xmltodict.parse(resp.text)
        state = str(dictResp['jobInfo']['state'])

        return state == ThreadedBulkPull.STATE_CLOSED

    ## SFDC main methods ##
    # Expected usage is a call to sfdcStartSession() followed by 
    # sfdcCreateWorkers(), followed by sfdcCreateWork()

    def sfdcStartSession(self, username, password, token):
        """SFDC API instantiation is essentially loggin in
        param:  username
        type:   str, SFDC login username
        param:  password
        type:   str, SFDC login password
        param:  token
        type:   str, SFDC login token
        """
        session_info = self.apiLogin(username, password, token)
        self.session_id = session_info[0]
        self.instance = session_info[1]

    def sfdcCreateWork(self, query, chunkSize=10000, **kwargs):
        """Creates a job, an initial batch based on a general query, allows
        the API to create all the batches it needs, and closes the job.
        """
        job_id = self.sfdcCreateJob(**kwargs)
        initial_batch = self.sfdcCreateBatch(query, chunkSize, **kwargs)
        all_batches = self.sfdcGetBatches(**kwargs)
        num_batches = len(all_batches)
        closed = self.sfdcCloseJob(**kwargs)

        batch_queue = Queue(maxsize=num_batches)
        for batch_id in all_batches:
            batch_queue.put(batch_id)

        batch_queue.join()

    @staticmethod
    def sfdcWorker(work_queue, session_args):
        """Function executed by thread will pull a batch and check it's status.
        If the batch is ready, the results will be retrieved and dumped
        into a file. If not they're added back into the queue. If they've
        failed, they're abandonned.
        param:  work_queue
        type:   Queue
        param:  session_args
        type:   dict
        """
        batch_id = work_queue.get()
        batch_status = ThreadedBulkPull.sfdcBatchStatus(batch_id, **session_args)
        batch_state = batch_status.get('state', '')
        batch_dir = session_args.get('batch_dir', '')
        batch_name = '/batch_' + threading.currentThread().getName() + '_' +\
                     datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        if batch_state == 'Completed':
            with open(batch_dir + batch_name, 'w') as f: 
                writer = csv.writer(f)
                for row in ThreadedBulkPull.sfdcRetrieveData(batch_id,\
                                                             **session_args):
                    writer.writerow(row)
        elif batch_state in ['Queued', 'InProgress']:
            work_queue.put(batch_id)
            # potentially wait for bit
        elif batch_state in ['Failed', 'Not Processed']:
            pass

        work_queue.task_done()

    @staticmethod
    def sfdcCreateWorkers(num_workers, work_queue, session_args):
        """Generates a pool of workers and starts them. They'll run until
        the main instance is closed looking for work in the provided queue.
        param:  num_workers
        type:   int
        param:  work_queue
        type:   Queue
        param:  session_args
        type:   dict
        """
        for _ in range(num_workers):
            worker = Thread(target=ThreadedBulkPull.sfdcWorker, args=\
                            (work_queue, session_args,))
            worker.daemon = True
            worker.start()

        return True

#!/usr/bin/env python3
"""Exercise rendered hub templates against public Kubernetes API fixtures.

Requires Helm, Go and PyYAML. This does not exercise live policy reconciliation.
"""
import copy
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml

CHART = Path(__file__).resolve().parents[1]
MC_KEY = 'cluster.open-cluster-management.io/v1/ManagedCluster//'
WORK_PREFIX = 'work.open-cluster-management.io/v1/ManifestWork/'
CRD_PREFIX = 'apiextensions.k8s.io/v1/CustomResourceDefinition//'


def ready_crds():
    return {CRD_PREFIX + name: {'status': {'conditions': [
        {'type': 'Established', 'status': 'True'}]}}
        for name in ('gridnetworks.grid.praxis.fast', 'gridsites.grid.praxis.fast')}


def member(name, selected=True, cluster_set='default'):
    return {'metadata': {'name': name, 'labels': {
        'grid.praxis.fast/member': str(selected).lower(),
        'cluster.open-cluster-management.io/clusterset': cluster_set}}}


def work(site, **changes):
    fields = {'siteName': site, 'networkName': 'grid',
              'gatewayAddress': f'{site}.example.test:443',
              'signalsAddress': f'{site}.example.test:443',
              'signalsServerName': f'{site}-signals.grid.internal'}
    fields.update(changes)
    return {'status': {'resourceStatus': {'manifests': [{
        'statusFeedback': {'values': [
            {'name': key, 'fieldValue': {'type': 'String', 'string': value}}
            for key, value in fields.items() if value is not None]}}]}}}


class ManagedClusterTemplates(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='grid-managed-test-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.renderer = str(Path(cls.temp.name) / 'render')
        subprocess.run(['go', 'build', '-o', cls.renderer,
                        str(CHART / 'tests/render-hub-template.go')], check=True)
        cls.values = {'mode': 'external', 'fullnameOverride': 'grid-bootstrap',
                      'grid': {'membership': {'source': 'Local'}},
                      'managedClusters': {'enabled': True},
                      'external': {'delivery': {'namespace': 'grid-system'},
                                   'certManager': {'enabled': True,
                                                   'issuer': {'create': True}}}}
        cls.docs = cls.render_chart(cls.values)
        cls.policies = {d['metadata']['name'].removeprefix('grid-bootstrap-membership'):
                        d['spec']['policy-templates'][0]['objectDefinition']['spec']
                        for d in cls.docs if d['kind'] == 'Policy'}

    @classmethod
    def render_chart(cls, values, chart=CHART):
        result = subprocess.run(['helm', 'template', 'grid-bootstrap', str(chart),
                                 '-n', 'grid-system', '-f', '-'],
                                input=yaml.safe_dump(values), text=True,
                                capture_output=True, check=True)
        return [d for d in yaml.safe_load_all(result.stdout) if d]

    def render(self, suffix='', resources=None, cluster='west', definitions_only=True):
        policy = self.policies[suffix]
        raw = policy.get('object-templates-raw') or yaml.safe_dump(policy['object-templates'])
        result = subprocess.run([self.renderer], input=json.dumps({
            'Template': raw, 'Cluster': cluster,
            'Resources': resources or {MC_KEY: {'items': []}}}),
            text=True, capture_output=True, check=True)
        managed = subprocess.run([self.renderer], input=json.dumps({
            'Template': result.stdout, 'Managed': True,
            'Resources': ready_crds()}), text=True, capture_output=True, check=True)
        objects = yaml.safe_load(managed.stdout)
        return [obj['objectDefinition'] for obj in objects] if definitions_only else objects

    def inventory(self):
        return {MC_KEY: {'items': [member('west'), member('east'), member('unselected', False),
                                  member('other-grid', cluster_set='other')]},
                WORK_PREFIX + 'east/grid-site-observer': work('east')}

    def peers(self, resources):
        return [o for o in self.render(resources=resources) if o['kind'] == 'GridSite']

    def test_empty_inventory_keeps_site_config_without_peers(self):
        config, = [o for o in self.render() if o['kind'] == 'ConfigMap']
        self.assertEqual(yaml.safe_load(config['data']['config.yaml']),
                         {'version': 1, 'site': {'name': 'west'}})

    def test_work_agent_can_observe_sites_without_write_permissions(self):
        role, = [o for o in self.render() if o['kind'] == 'ClusterRole']
        self.assertEqual(role['metadata']['name'], 'grid-site-observer-reader')
        self.assertEqual(role['metadata']['labels'],
                         {'open-cluster-management.io/aggregate-to-work': 'true'})
        self.assertEqual(role['rules'], [{'apiGroups': ['grid.praxis.fast'],
                                         'resources': ['gridsites'],
                                         'verbs': ['get', 'list', 'watch']}])
        self.assertEqual(self.policies['']['pruneObjectBehavior'], 'DeleteAll')

    def test_peer_delivery_waits_for_crds_before_emitting_any_objects(self):
        raw = self.policies['']['object-templates-raw']
        hub = subprocess.run([self.renderer], input=json.dumps({
            'Template': raw, 'Cluster': 'west', 'Resources': self.inventory()}),
            text=True, capture_output=True, check=True)
        managed = subprocess.run([self.renderer], input=json.dumps({
            'Template': hub.stdout, 'Managed': True, 'Resources': {}}),
            text=True, capture_output=True)
        self.assertNotEqual(managed.returncode, 0)
        self.assertEqual(managed.stdout, '')
        self.assertIn('waiting for CRD gridnetworks.grid.praxis.fast', managed.stderr)

    def test_remote_only_selected_members(self):
        peer, = self.peers(self.inventory())
        self.assertEqual(peer['metadata']['name'], 'east')
        self.assertEqual(peer['spec']['gatewayEndpoint']['tls']['serverName'], 'east.grid.internal')
        self.assertNotIn('status', peer)
        self.assertEqual(peer['metadata']['labels']['grid.praxis.fast/auto-discovered'], 'false')

    def test_peer_spec_removes_legacy_alias_without_removing_controller_metadata(self):
        objects = self.render(resources=self.inventory(), definitions_only=False)
        peer, = [o for o in objects if o['objectDefinition']['kind'] == 'GridSite']
        self.assertEqual(peer['complianceType'], 'mustonlyhave',
                         'omitting egress from a musthave template retains its old value')
        self.assertEqual(peer['metadataComplianceType'], 'musthave',
                         'other controllers retain ownership of labels and annotations')
        self.assertNotIn('egress', peer['objectDefinition']['spec'],
                         'even egress:null conflicts with the gatewayEndpoint alias')
        self.assertIn('gatewayEndpoint', peer['objectDefinition']['spec'])
        self.assertNotIn('status', peer['objectDefinition'])
        self.assertTrue(all(o['complianceType'] == 'musthave' for o in objects
                            if o['objectDefinition']['kind'] != 'GridSite'))

    def test_endpoint_update_and_optional_field_removal(self):
        resources = self.inventory()
        resources[WORK_PREFIX + 'east/grid-site-observer'] = work(
            'east', gatewayAddress='moved.example.test:443', region='eu', signalsAddress=None,
            signalsServerName=None)
        peer, = self.peers(resources)
        self.assertEqual(peer['spec']['gatewayEndpoint']['address'], 'moved.example.test:443')
        self.assertEqual(peer['spec']['region'], 'eu')
        self.assertIsNone(peer['spec']['signalsEndpoint'])
        self.assertIsNone(peer['spec']['trust'])
        self.assertIsNone(peer['spec']['zone'])

    def test_withdrawal_missing_feedback_and_identity_mismatch(self):
        for replacement in ({}, work('impostor'), work('east', networkName='other'),
                            work('east', gatewayAddress=None)):
            with self.subTest(replacement=replacement):
                resources = self.inventory()
                resources[WORK_PREFIX + 'east/grid-site-observer'] = replacement
                self.assertEqual(self.peers(resources), [])
        resources = self.inventory()
        resources[MC_KEY]['items'] = [member('west')]
        self.assertEqual(self.peers(resources), [])
        self.assertEqual(self.policies['']['pruneObjectBehavior'], 'DeleteAll')

    def test_bootstrap_issues_identities_and_read_only_observers(self):
        objects = self.render('-hub', self.inventory())
        certs = [o for o in objects if o['kind'] == 'Certificate']
        self.assertEqual({c['metadata']['name'] for c in certs}, {'grid-site-west', 'grid-site-east'})
        self.assertEqual(certs[0]['spec']['uris'], ['spiffe://grid.internal/site/west'])
        self.assertIn('west-signals.grid.internal', certs[0]['spec']['dnsNames'])
        self.assertEqual(certs[0]['spec']['renewBefore'], '1440h')
        works = [o for o in objects if o['kind'] == 'ManifestWork']
        self.assertEqual(len(works), 2)
        for observer in works:
            cfg, = observer['spec']['manifestConfigs']
            self.assertEqual(cfg['updateStrategy']['type'], 'ReadOnly')
            self.assertEqual(cfg['feedbackScrapeType'], 'Watch')
            self.assertTrue(all(p['path'].startswith(('.spec.', '.metadata.'))
                                for p in cfg['feedbackRules'][0]['jsonPaths']))
        self.assertFalse(any(o['kind'] == 'Secret' for o in objects))

    def test_management_only_hub_is_separate_from_grid_membership(self):
        resources = {MC_KEY: {'items': [member('local-cluster', False), member('east')]}}
        objects = self.render('-hub', resources)
        certs = [o for o in objects if o['kind'] == 'Certificate']
        self.assertEqual([c['metadata']['name'] for c in certs], ['grid-site-east'])
        observers = [o for o in objects if o['kind'] == 'ManifestWork']
        self.assertEqual([o['metadata']['namespace'] for o in observers], ['east'])
        placements = {d['metadata']['name']: d['spec'] for d in self.docs if d['kind'] == 'Placement'}
        hub = placements['grid-bootstrap-membership-hub']['predicates'][0]
        members = placements['grid-bootstrap-membership']['predicates'][0]
        self.assertEqual(hub['requiredClusterSelector']['labelSelector']['matchLabels'],
                         {'name': 'local-cluster'})
        self.assertEqual(members['requiredClusterSelector']['labelSelector']['matchLabels'],
                         {'grid.praxis.fast/member': 'true'})

    def test_observer_access_does_not_wait_for_hub_policy(self):
        for work_name in ('grid-site-observer', 'custom-observer'):
            with self.subTest(work_name=work_name):
                values = copy.deepcopy(self.values)
                values['managedClusters']['workName'] = work_name
                docs = self.render_chart(values)
                policy = next(d for d in docs if d['kind'] == 'Policy' and
                              d['metadata']['name'] == 'grid-bootstrap-membership')
                account = policy['spec']['hubTemplateOptions']['serviceAccountName']
                binding, = [d for d in docs if d['kind'] == 'ClusterRoleBinding' and
                            {'kind': 'ServiceAccount', 'name': account,
                             'namespace': policy['metadata']['namespace']} in d['subjects']]
                role, = [d for d in docs if d['kind'] == binding['roleRef']['kind'] and
                         d['metadata']['name'] == binding['roleRef']['name']]
                observer_rules = [r for r in role['rules'] if
                                  'manifestworks' in r['resources']]
                self.assertEqual(len(observer_rules), 1,
                                 'observer watches must be authorized before hub policy enforcement')
                rule, = observer_rules
                self.assertEqual(rule['apiGroups'], ['work.open-cluster-management.io'])
                self.assertEqual(rule['resources'], ['manifestworks'])
                self.assertEqual(rule['resourceNames'], [work_name])
                self.assertEqual(set(rule['verbs']), {'get', 'list', 'watch'})
                self.assertTrue(all(r['resources'] in (['managedclusters'], ['manifestworks'])
                                    for r in role['rules']))

    def test_permission_changes_refresh_hub_templates_without_spec_changes(self):
        def triggers(docs):
            return {d['metadata']['name']: d['metadata']['annotations'][
                    'policy.open-cluster-management.io/trigger-update']
                    for d in docs if d['kind'] == 'Policy' and
                    'hubTemplateOptions' in d['spec']}

        original = triggers(self.docs)
        self.assertEqual(len(original), 2)
        self.assertEqual(original, triggers(self.render_chart(self.values)))
        values = copy.deepcopy(self.values)
        values['managedClusters']['identitySecretPrefix'] = 'another-site-'
        self.assertEqual(original, triggers(self.render_chart(values)))
        with tempfile.TemporaryDirectory(prefix='grid-rbac-refresh-test-') as temp:
            chart = Path(temp) / 'chart'
            shutil.copytree(CHART, chart)
            helper = chart / 'templates/_managed-clusters.tpl'
            helper.write_text(helper.read_text().replace(
                'verbs: [get, list, watch]', 'verbs: [get, list]', 1))
            changed = self.render_chart(self.values, chart)
        for name, trigger in triggers(changed).items():
            self.assertNotEqual(original[name], trigger)
        self.assertEqual([d['spec'] for d in self.docs if d['kind'] == 'Policy'],
                         [d['spec'] for d in changed if d['kind'] == 'Policy'])

    def test_decommission_stops_renewal_without_owning_ca(self):
        objects = self.render('-hub')
        self.assertEqual([o['kind'] for o in objects], ['Namespace'])
        self.assertEqual(self.policies['-hub']['pruneObjectBehavior'], 'DeleteIfCreated')
        self.assertEqual(self.policies['-identities']['pruneObjectBehavior'], 'None')

    def test_identity_delivery_never_exports_signing_key(self):
        _, identity, bundle = self.render('-identities')
        self.assertEqual(identity['metadata']['annotations']['cert-manager.io/certificate-name'],
                         'grid-site-west')
        self.assertEqual(identity['data']['tls.key'], 'grid-system/grid-site-west/tls.key')
        self.assertEqual(bundle['data'], {'ca.crt': 'grid-system/grid-ca-key/tls.crt'})

    def test_no_renderer_specific_resources_or_switches(self):
        self.assertFalse(any(d['apiVersion'].startswith('argoproj.io/') for d in self.docs))
        self.assertFalse(any(d['kind'] == 'Secret' for d in self.docs))
        role, = [d for d in self.docs if d['kind'] == 'ClusterRole']
        self.assertEqual(role['rules'][0]['resources'], ['managedclusters'])
        self.assertEqual(role['rules'][0]['verbs'], ['get', 'list', 'watch'])

    def test_external_pki_can_supply_the_same_inventory_identity_names(self):
        values = copy.deepcopy(self.values)
        values['external']['certManager']['enabled'] = False
        values['external']['trustBundleSecretRef'] = 'public-grid-ca'
        docs = self.render_chart(values)
        policies = [d for d in docs if d['kind'] == 'Policy']
        rendered = yaml.safe_dump(policies)
        self.assertNotIn('kind: Certificate', rendered)
        self.assertNotIn('cert-manager.io/certificate-name', rendered)
        self.assertIn('public-grid-ca', rendered)

    def test_enrollment_creates_complete_network_after_identity_delivery(self):
        values = copy.deepcopy(self.values)
        values['gridIdentity'] = {'enabled': True}
        docs = self.render_chart(values)
        policy, = [d for d in docs if d['kind'] == 'Policy' and
                   d['metadata']['name'] == 'grid-bootstrap-membership']
        spec = policy['spec']['policy-templates'][1]['objectDefinition']['spec']
        self.assertEqual(spec['pruneObjectBehavior'], 'None')

        def execute(raw, resources, managed=False):
            return subprocess.run([self.renderer], input=json.dumps({
                'Template': raw, 'Cluster': 'west', 'Managed': managed,
                'Resources': resources}), text=True, capture_output=True)

        state = {'v1/ConfigMap/grid-system/grid-bootstrap-state':
                 {'data': {'gridId': 'persisted-grid'}}}
        hub = execute(spec['object-templates-raw'], state)
        self.assertEqual(hub.returncode, 0, hub.stderr)
        identities = {'v1/Secret/grid-system/grid-site-identity':
                      {'metadata': {'name': 'grid-site-identity'}},
                      'v1/Secret/grid-system/grid-ca': {'metadata': {'name': 'grid-ca'}}}
        managed = execute(hub.stdout, {**ready_crds(), **identities}, managed=True)
        self.assertEqual(managed.returncode, 0, managed.stderr)
        objects = yaml.safe_load(managed.stdout)
        self.assertEqual(objects[0]['objectDefinition']['spec'], {
            'gridId': 'persisted-grid',
            'membership': {'mode': 'Local'},
            'signalTransport': {'mode': 'poll'},
            'peerTrust': {'mode': 'spiffe'},
            'tls': {'siteSecretRef': {'name': 'grid-site-identity', 'namespace': 'grid-system'},
                    'caSecretRef': {'name': 'grid-ca', 'namespace': 'grid-system'}},
            'consumerGateways': [{'name': 'grid-gateway', 'namespace': 'grid-system', 'siteName': 'west'}]})
        self.assertEqual(len(objects), 1)
        for name in ('gridnetworks.grid.praxis.fast', 'gridsites.grid.praxis.fast'):
            for conditions in (None, [], [{'type': 'Established', 'status': 'False'}]):
                resources = {**ready_crds(), **identities}
                if conditions is None:
                    del resources[CRD_PREFIX + name]
                else:
                    resources[CRD_PREFIX + name] = {'status': {'conditions': conditions}}
                blocked = execute(hub.stdout, resources, managed=True)
                self.assertNotEqual(blocked.returncode, 0)
                self.assertEqual(blocked.stdout, '', 'waiting must emit no objects to write')
                self.assertIn('waiting for CRD ' + name, blocked.stderr)
        missing = execute(hub.stdout, ready_crds(), managed=True)
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn('waiting for the enrolled site identity', missing.stderr)
        missing_ca = execute(hub.stdout, {**ready_crds(), 'v1/Secret/grid-system/grid-site-identity':
                                         identities['v1/Secret/grid-system/grid-site-identity']}, managed=True)
        self.assertNotEqual(missing_ca.returncode, 0)
        self.assertIn('waiting for the grid trust bundle', missing_ca.stderr)
        self.assertNotEqual(execute(spec['object-templates-raw'], {}).returncode, 0)
        state['v1/ConfigMap/grid-system/grid-bootstrap-state']['data']['gridId'] = ''
        empty = execute(spec['object-templates-raw'], state)
        self.assertNotEqual(empty.returncode, 0)
        self.assertIn('waiting for bootstrap grid identity', empty.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)

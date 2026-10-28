#!/usr/bin/env python3
"""Exercise rendered hub templates against public Kubernetes API fixtures.

Requires Helm, Go and PyYAML. This does not exercise live policy reconciliation.
"""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml

CHART = Path(__file__).resolve().parents[1]
MC_KEY = 'cluster.open-cluster-management.io/v1/ManagedCluster//'
WORK_PREFIX = 'work.open-cluster-management.io/v1/ManifestWork/'


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
    def render_chart(cls, values):
        result = subprocess.run(['helm', 'template', 'grid-bootstrap', str(CHART),
                                 '-n', 'grid-system', '-f', '-'],
                                input=yaml.safe_dump(values), text=True,
                                capture_output=True, check=True)
        return [d for d in yaml.safe_load_all(result.stdout) if d]

    def render(self, suffix='', resources=None, cluster='west'):
        policy = self.policies[suffix]
        raw = policy.get('object-templates-raw') or yaml.safe_dump(policy['object-templates'])
        result = subprocess.run([self.renderer], input=json.dumps({
            'Template': raw, 'Cluster': cluster,
            'Resources': resources or {MC_KEY: {'items': []}}}),
            text=True, capture_output=True, check=True)
        return [obj['objectDefinition'] for obj in yaml.safe_load(result.stdout)]

    def inventory(self):
        return {MC_KEY: {'items': [member('west'), member('east'), member('unselected', False),
                                  member('other-grid', cluster_set='other')]},
                WORK_PREFIX + 'east/grid-site-observer': work('east')}

    def peers(self, resources):
        return [o for o in self.render(resources=resources) if o['kind'] == 'GridSite']

    def test_empty_inventory_keeps_site_config_without_peers(self):
        config, = self.render()
        self.assertEqual(yaml.safe_load(config['data']['config.yaml']),
                         {'version': 1, 'site': {'name': 'west'}})

    def test_remote_only_selected_members(self):
        peer, = self.peers(self.inventory())
        self.assertEqual(peer['metadata']['name'], 'east')
        self.assertEqual(peer['spec']['gatewayEndpoint']['tls']['serverName'], 'east.grid.internal')
        self.assertNotIn('status', peer)
        self.assertEqual(peer['metadata']['labels']['grid.praxis.fast/auto-discovered'], 'false')

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
        roles = [o for o in objects if o['kind'] == 'Role']
        self.assertEqual(roles[0]['rules'][0]['resourceNames'], ['grid-site-observer'])
        self.assertFalse(any(o['kind'] == 'Secret' for o in objects))

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


if __name__ == '__main__':
    unittest.main(verbosity=2)

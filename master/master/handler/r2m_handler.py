from api.m2r import r2m
from region_cluster import RegionCluster


class R2MHandler(r2m.Iface):
    def __init__(self, cluster):
        assert isinstance(cluster, RegionCluster)

        self._cluster = cluster

    def createResp(self, state, aid):
        if state == 0:
            self._cluster.do_assign(aid)

    def dropResp(self, state, aid):
        if state == 0:
            self._cluster.do_assign(aid)

    def recoverResp(self, state, aid):
        if state == 0:
            self._cluster.do_assign(aid)

    def upgradeResp(self, state, aid):
        if state == 0:
            self._cluster.do_assign(aid)
